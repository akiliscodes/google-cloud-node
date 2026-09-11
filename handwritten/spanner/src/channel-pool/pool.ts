/*!
 * Copyright 2026 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as grpc from '@grpc/grpc-js';
import {context, ROOT_CONTEXT} from '@opentelemetry/api';
import {TransactionAffinity} from './affinity';
import {ChannelPoolOptions, normalizeChannelPoolOptions} from './config';
import {ChannelEntry, ChannelLease, ChannelState} from './entry';
import {
  allocateSlot,
  calculateScaleUpCount,
  dialAndPrimeChannel,
  evaluateScaleDown,
  sweepDrainingChannels,
} from './scaler';

export type ChannelFactory = (
  target: string,
  credentials: grpc.ChannelCredentials,
  options: grpc.ChannelOptions,
) => grpc.Channel;

/**
 * Native dynamically scaling gRPC channel pool for Spanner.
 *
 * Implements the gRPC Channel interface so it can be passed directly as a channel override
 * to GAPIC stubs, while dynamically routing calls across channels via Power of Two Choices
 * (P2C) load balancing and caller-owned transaction affinity.
 */
export class ChannelPool {
  readonly config: Required<ChannelPoolOptions>;
  readonly target: string;
  readonly credentials: grpc.ChannelCredentials;
  readonly channelOptions: grpc.ChannelOptions;

  private readonly activeEntries: ChannelEntry[] = [];
  private readonly drainingEntries: ChannelEntry[] = [];
  private readonly channelFactory: ChannelFactory;
  private readonly primeExecutor?: (
    channel: grpc.Channel,
    sessionName: string,
    timeoutMs: number,
  ) => Promise<void>;

  private nextEntryId = 1;
  private consecutiveLowLoadChecks = 0;
  private lastScaleUpTime = 0;
  private scaleUpInProgress = false;
  private _primeSession?: string;
  private scaleDownTimer?: NodeJS.Timeout;
  private closed = false;

  constructor(
    target: string,
    credentials: grpc.ChannelCredentials,
    channelOptions: grpc.ChannelOptions = {},
    options?: ChannelPoolOptions,
    customChannelFactory?: ChannelFactory,
    primeExecutor?: (
      channel: grpc.Channel,
      sessionName: string,
      timeoutMs: number,
    ) => Promise<void>,
  ) {
    this.target = target;
    this.credentials = credentials;
    const sanitizedOptions = {...channelOptions};
    delete (sanitizedOptions as any)['grpc.spanner_channel_pool_holder'];
    delete (sanitizedOptions as any)['grpc.spanner_channel_pool_options'];
    delete (sanitizedOptions as any)['grpc.channelFactoryOverride'];
    delete (sanitizedOptions as any)['grpc.callInvocationTransformer'];
    this.channelOptions = sanitizedOptions;
    this.config = normalizeChannelPoolOptions(options);
    this.channelFactory =
      customChannelFactory ||
      ((target, credentials, options) =>
        new (grpc.Channel as any)(target, credentials, options));
    this.primeExecutor = primeExecutor;

    // Eagerly initialize initialChannels
    for (let index = 0; index < this.config.initialChannels; index++) {
      const logicalChannelId = index + 1;
      const channel = this.channelFactory(
        this.target,
        this.credentials,
        this.channelOptions,
      );
      const entry = new ChannelEntry(
        this.nextEntryId++,
        logicalChannelId,
        channel,
      );
      this.activeEntries.push(entry);
    }

    // Start background scale-down monitor only if dynamic scaling is active
    if (this.config.minChannels < this.config.maxChannels) {
      this.startScaleDownMonitor();
    }
  }

  /**
   * Returns the count of active channels in the pool.
   */
  activeChannelCount(): number {
    return this.activeEntries.length;
  }

  /**
   * Returns the count of draining channels in the pool.
   */
  drainingChannelCount(): number {
    return this.drainingEntries.length;
  }

  /**
   * Returns a copy of the active channel entries.
   */
  activeChannels(): ChannelEntry[] {
    return [...this.activeEntries];
  }

  /**
   * Returns a copy of the draining channel entries.
   */
  drainingChannels(): ChannelEntry[] {
    return [...this.drainingEntries];
  }

  /**
   * Returns true if dynamic scaling is enabled (minChannels < maxChannels).
   */
  isDynamic(): boolean {
    return this.config.minChannels < this.config.maxChannels;
  }

  /**
   * Returns the total count of in-flight RPCs across all active channels.
   */
  totalInFlightRpcs(): number {
    return this.activeEntries.reduce(
      (total, entry) => total + entry.inFlight(),
      0,
    );
  }

  /**
   * Sets the multiplexed session name used for warming new channels via SELECT 1.
   *
   * @param sessionName Formatted multiplexed session name string.
   */
  setPrimeSession(sessionName: string): void {
    this._primeSession = sessionName;
  }

  /**
   * Clears the registered prime session name.
   */
  clearPrimeSession(): void {
    this._primeSession = undefined;
  }

  /**
   * Checks if a multiplexed session name is currently registered for priming.
   */
  hasPrimeSession(): boolean {
    return this._primeSession !== undefined;
  }

  get primeSession(): string | undefined {
    return this._primeSession;
  }

  /**
   * Selects an active channel using Power of Two Choices (P2C) least-busy load balancing.
   *
   * Compares 2 random choices based on:
   * 1. in_flight_rpcs (lowest load)
   * 2. active_rw_transactions (tie-breaker 1)
   * 3. uniform random selection (tie-breaker 2; warmth tie-breaking is deliberately
   *    avoided to eliminate the hot-channel trap under sequential traffic)
   */
  pickChannel(): ChannelLease {
    if (this.activeEntries.length === 0) {
      throw new Error('ChannelPool has no active channels');
    }

    let selectedEntry: ChannelEntry;

    if (this.activeEntries.length === 1) {
      selectedEntry = this.activeEntries[0];
    } else {
      const index1 = Math.floor(Math.random() * this.activeEntries.length);
      let index2 = Math.floor(Math.random() * (this.activeEntries.length - 1));
      if (index2 >= index1) {
        index2 += 1;
      }

      const entry1 = this.activeEntries[index1];
      const entry2 = this.activeEntries[index2];

      const load1 = entry1.inFlight();
      const load2 = entry2.inFlight();

      if (load1 < load2) {
        selectedEntry = entry1;
      } else if (load2 < load1) {
        selectedEntry = entry2;
      } else {
        // Tie-breaker 1: Active Read/Write transactions
        const readWriteCount1 = entry1.activeReadWriteTransactionCount();
        const readWriteCount2 = entry2.activeReadWriteTransactionCount();
        if (readWriteCount1 < readWriteCount2) {
          selectedEntry = entry1;
        } else if (readWriteCount2 < readWriteCount1) {
          selectedEntry = entry2;
        } else {
          // Tie-breaker 2: Uniformly random selection between sampled pair.
          // Note: Channel warmth (most recently active) is deliberately NOT used here
          // to prevent the "hot-channel trap" where a released channel repeatedly starves
          // other idle channels under sequential traffic patterns.
          selectedEntry = Math.random() < 0.5 ? entry1 : entry2;
        }
      }
    }

    return this.makeLease(selectedEntry);
  }

  /**
   * Resolves an affinity handle to a leased channel.
   *
   * Behavior:
   * - If affinity is unpinned: Selects a channel via P2C and pins affinity via CAS.
   * - If affinity is pinned (Hard Stickiness - Read/Write): Returns pinned channel if active or draining.
   * - If affinity is pinned (Soft Stickiness - Read-Only): Returns pinned channel if active; if draining,
   *   re-pins to a fresh active channel.
   * - If CAS contention occurs: Resolves conflict via resolveCasConflict.
   *
   * @param affinity Optional transaction affinity handle.
   */
  acquire(affinity?: TransactionAffinity): ChannelLease {
    if (!affinity) {
      return this.pickChannel();
    }

    const currentId = affinity.pinnedEntryId();

    if (currentId !== null) {
      // 1. Fast-path: Check activeEntries for pinned channel
      const activeMatch = this.activeEntries.find(
        entry => entry.id === currentId && entry.isActive(),
      );
      if (activeMatch) {
        const lease = this.makeLease(activeMatch);
        if (affinity.isReadWrite()) {
          affinity.ensureReadWriteGuard(lease);
        }
        return lease;
      }

      // 2. Draining-path: Only Read/Write transactions (hard stickiness) preserve draining affinity
      if (affinity.isReadWrite()) {
        const drainingMatch = this.drainingEntries.find(
          entry => entry.id === currentId && !entry.isClosed(),
        );
        if (drainingMatch) {
          const lease = this.makeLease(drainingMatch);
          affinity.ensureReadWriteGuard(lease);
          return lease;
        }
      }
    }

    // 3. Selection: Select a fresh channel from activeEntries
    const lease = this.pickChannel();
    const expectedId = currentId ?? 0;

    const casResult = affinity.compareAndSetEntryId(expectedId, lease.entryId);
    let finalLease: ChannelLease;

    if (casResult.success) {
      finalLease = lease;
    } else {
      finalLease = this.resolveCasConflict(affinity, lease, casResult.winnerId);
    }

    if (affinity.isReadWrite()) {
      affinity.ensureReadWriteGuard(finalLease);
    }

    return finalLease;
  }

  /**
   * Resolves concurrent CAS races when multiple statements attempt to pin affinity simultaneously.
   *
   * Matches the reference Rust implementation:
   * 1. Try to find the winner in active candidates.
   * 2. If Read/Write, try to find the winner in draining entries.
   * 3. If the winner is unusable (closed or draining for Read-Only), attempt to re-pin affinity
   *    to our active lease via CAS. If re-pinning encounters another concurrent race, loop and retry.
   */
  resolveCasConflict(
    affinity: TransactionAffinity,
    lease: ChannelLease,
    initialWinnerId: number,
  ): ChannelLease {
    let winnerId = initialWinnerId;

    let resolvedLease: ChannelLease | undefined;
    while (!resolvedLease) {
      // 1. Try to find winner in active entries
      const activeWinner = this.activeEntries.find(
        entry => entry.id === winnerId && entry.isActive(),
      );
      if (activeWinner) {
        lease.dispose();
        resolvedLease = this.makeLease(activeWinner);
        break;
      }

      // 2. Try to find winner in draining entries for Read/Write transactions
      if (affinity.isReadWrite()) {
        const drainingWinner = this.drainingEntries.find(
          entry => entry.id === winnerId && entry.isDraining(),
        );
        if (drainingWinner) {
          lease.dispose();
          resolvedLease = this.makeLease(drainingWinner);
          break;
        }
      }

      // 3. Winner is unusable (closed, or draining for Read-Only).
      // Attempt to re-pin affinity to our active lease.
      const casResult = affinity.compareAndSetEntryId(winnerId, lease.entryId);
      if (casResult.success) {
        resolvedLease = lease;
        break;
      }
      winnerId = casResult.winnerId;
    }

    return resolvedLease;
  }

  /**
   * Triggers an asynchronous scale-up evaluation and parallel channel dial & prime.
   */
  async triggerScaleUp(): Promise<void> {
    if (this.closed || this.scaleUpInProgress) {
      return;
    }

    const now = Date.now();
    if (now - this.lastScaleUpTime < this.config.scaleUpCooldownMs) {
      return;
    }

    const currentLen = this.activeEntries.length;
    const totalInFlight = this.totalInFlightRpcs();
    const channelsToAdd = calculateScaleUpCount(
      currentLen,
      totalInFlight,
      this.config,
    );

    if (channelsToAdd <= 0) {
      return;
    }

    this.scaleUpInProgress = true;
    this.lastScaleUpTime = now;

    try {
      const dialPromises: Array<Promise<grpc.Channel>> = [];
      for (let i = 0; i < channelsToAdd; i++) {
        dialPromises.push(
          dialAndPrimeChannel(
            this.target,
            this.credentials,
            this.channelOptions,
            this.primeSession,
            this.config.primeTimeoutMs,
            this.channelFactory,
            this.primeExecutor,
          ),
        );
      }

      const results = await Promise.allSettled(dialPromises);
      for (const result of results) {
        if (result.status !== 'fulfilled') {
          const reason: any = result.reason;
          if (
            reason?.code === (grpc.status as any).NOT_FOUND ||
            (typeof reason?.message === 'string' &&
              (reason.message.includes('Session not found') ||
                reason.message.includes('NOT_FOUND')))
          ) {
            this.clearPrimeSession();
          }
          continue;
        }
        const channel = result.value;
        if (this.closed) {
          try {
            channel.close();
          } catch {
            // Ignore
          }
          continue;
        }
        this.publishPrimedChannel(channel);
      }
    } finally {
      this.scaleUpInProgress = false;
    }
  }

  /**
   * Publishes a newly primed channel into the active pool, allocating the lowest available slot.
   */
  publishPrimedChannel(channel: grpc.Channel): ChannelEntry | null {
    if (this.activeEntries.length >= this.config.maxChannels) {
      try {
        channel.close();
      } catch {
        // Ignore channel close error
      }
      return null;
    }

    const occupiedSlots = new Set<number>();
    for (const entry of this.activeEntries) {
      occupiedSlots.add(entry.logicalChannelId);
    }
    for (const entry of this.drainingEntries) {
      if (!entry.isClosed()) {
        occupiedSlots.add(entry.logicalChannelId);
      }
    }

    const logicalChannelId = allocateSlot(
      occupiedSlots,
      this.config.maxChannels,
    );
    const entry = new ChannelEntry(
      this.nextEntryId++,
      logicalChannelId,
      channel,
    );
    this.activeEntries.push(entry);
    return entry;
  }

  /**
   * Executes a periodic scale-down check and draining sweep.
   */
  scaleDownEvaluation(): void {
    if (this.closed) {
      return;
    }

    // 1. Evaluate utilization and transition excess channels to DRAINING if debounced
    const evalResult = evaluateScaleDown(
      this.activeEntries,
      this.config,
      this.consecutiveLowLoadChecks,
    );
    this.consecutiveLowLoadChecks = evalResult.newLowLoadChecks;

    for (const candidate of evalResult.candidatesToRemove) {
      const index = this.activeEntries.indexOf(candidate);
      if (index !== -1) {
        this.activeEntries.splice(index, 1);
        candidate.setState(ChannelState.DRAINING);
        this.drainingEntries.push(candidate);
      }
    }

    // 2. Sweep draining channels and close those whose draining requirements are satisfied
    const sweepResult = sweepDrainingChannels(
      this.drainingEntries,
      this.config.drainIdleGraceMs,
    );

    this.drainingEntries.length = 0;
    this.drainingEntries.push(...sweepResult.retained);
  }

  private startScaleDownMonitor(): void {
    if (this.scaleDownTimer) {
      return;
    }
    this.scaleDownTimer = context.with(ROOT_CONTEXT, () =>
      setInterval(() => {
        this.scaleDownEvaluation();
      }, this.config.scaleDownIntervalMs),
    );

    // Unref timer so it does not block Node.js process termination
    if (this.scaleDownTimer.unref) {
      this.scaleDownTimer.unref();
    }
  }

  private stopScaleDownMonitor(): void {
    if (this.scaleDownTimer) {
      clearInterval(this.scaleDownTimer);
      this.scaleDownTimer = undefined;
    }
  }

  private makeLease(entry: ChannelEntry): ChannelLease {
    const lease = new ChannelLease(entry);
    if (
      this.config.minChannels < this.config.maxChannels &&
      entry.inFlight() > this.config.maxRpcPerChannel
    ) {
      void this.triggerScaleUp().catch(() => {});
    }
    return lease;
  }

  /**
   * Creates a CallInvocationTransformer for @grpc/grpc-js clients.
   */
  createCallInvocationTransformer(): (callProperties: any) => any {
    return callInvocationTransformer;
  }

  // --- grpc.Channel API Implementation ---

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.stopScaleDownMonitor();

    for (const entry of this.activeEntries) {
      entry.close();
    }
    this.activeEntries.length = 0;

    for (const entry of this.drainingEntries) {
      entry.close();
    }
    this.drainingEntries.length = 0;
  }

  getTarget(): string {
    return this.target;
  }

  getConnectivityState(tryToConnect: boolean): grpc.connectivityState {
    if (this.activeEntries.length === 0) {
      return (grpc.connectivityState as any).SHUTDOWN;
    }
    // Return READY if any channel is ready, otherwise first channel's state
    for (const entry of this.activeEntries) {
      const state = entry.channel.getConnectivityState(tryToConnect);
      if (state === (grpc.connectivityState as any).READY) {
        return state;
      }
    }
    return this.activeEntries[0].channel.getConnectivityState(tryToConnect);
  }

  watchConnectivityState(
    currentState: grpc.connectivityState,
    deadline: Date | number,
    callback: (error?: Error) => void,
  ): void {
    if (this.activeEntries.length === 0) {
      callback(new Error('ChannelPool has no active channels'));
      return;
    }
    this.activeEntries[0].channel.watchConnectivityState(
      currentState,
      deadline,
      callback,
    );
  }

  getChannelzRef(): any {
    if (this.activeEntries.length === 0) {
      return null;
    }
    return (this.activeEntries[0].channel as any).getChannelzRef?.() ?? null;
  }

  createCall(
    method: string,
    deadline: any,
    host: string | null | undefined,
    parentCall: any,
    propagateFlags: number | null | undefined,
  ): any {
    if (this.activeEntries.length === 0) {
      throw new Error('ChannelPool has no active channels');
    }
    const entry =
      this.activeEntries[Math.floor(Math.random() * this.activeEntries.length)];
    return (entry.channel as any).createCall(
      method,
      deadline,
      host,
      parentCall,
      propagateFlags,
    );
  }
}

/**
 * Updates the logical channel ID within an x-goog-spanner-request-id string without allocating arrays.
 * Format: version.processId.clientId.channelId.nthRequest.retryCount
 */
export function updateRequestIdChannel(
  requestId: string,
  logicalChannelId: number,
): string {
  let dotCount = 0;
  let start = -1;
  let end = -1;
  for (let i = 0; i < requestId.length; i++) {
    if (requestId.charCodeAt(i) === 46 /* '.' */) {
      dotCount++;
      if (dotCount === 3) {
        start = i + 1;
      } else if (dotCount === 4) {
        end = i;
        break;
      }
    }
  }
  if (start !== -1 && end !== -1) {
    return (
      requestId.substring(0, start) +
      logicalChannelId.toString() +
      requestId.substring(end)
    );
  }
  return requestId;
}

/**
 * Standalone CallInvocationTransformer for gRPC clients using ChannelPool.
 */
export function callInvocationTransformer(callProperties: any): any {
  if (
    !callProperties.channel ||
    !(callProperties.channel instanceof ChannelPool)
  ) {
    return callProperties;
  }

  const pool = callProperties.channel as ChannelPool;
  const affinity =
    callProperties.callOptions?.affinity ??
    callProperties.callOptions?.otherArgs?.options?.affinity;

  const lease = pool.acquire(affinity);

  // Post-process interceptor to release lease upon completion or cancellation
  const postProcessInterceptor = (options: any, nextCall: any) => {
    let disposed = false;
    const releaseLease = () => {
      if (!disposed) {
        disposed = true;
        lease.dispose();
      }
    };

    const requester = {
      start: (metadata: any, listener: any, next: any) => {
        const newListener = {
          onReceiveStatus: (status: any, nextStatus: any) => {
            releaseLease();
            nextStatus(status);
          },
        };
        try {
          next(metadata, newListener);
        } catch (error) {
          releaseLease();
          throw error;
        }
      },
      cancel: (next: any) => {
        releaseLease();
        next();
      },
    };

    try {
      return new (grpc as any).InterceptingCall(nextCall(options), requester);
    } catch (error) {
      releaseLease();
      throw error;
    }
  };

  const callOptions = callProperties.callOptions;
  const interceptors = callOptions?.interceptors;
  const newInterceptors = interceptors
    ? interceptors.concat(postProcessInterceptor)
    : [postProcessInterceptor];

  callProperties.channel = lease.channel;
  if (callOptions) {
    callOptions.interceptors = newInterceptors;
  } else {
    callProperties.callOptions = {interceptors: newInterceptors};
  }

  return callProperties;
}

export interface ChannelPoolHolder {
  pool?: ChannelPool;
  pools?: Map<string, ChannelPool>;
}

/**
 * Standard gRPC channelFactoryOverride callback creating or reusing a ChannelPool.
 */
export function channelFactoryOverride(
  address: string,
  credentials: grpc.ChannelCredentials,
  channelOptions: grpc.ChannelOptions,
): grpc.Channel {
  const holder = (channelOptions as any)?.[
    'grpc.spanner_channel_pool_holder'
  ] as ChannelPoolHolder | undefined;

  if (holder) {
    if (!holder.pools) {
      holder.pools = new Map();
      if (holder.pool) {
        holder.pools.set(holder.pool.target, holder.pool);
      }
    }
    const existing = holder.pools.get(address);
    if (existing) {
      return existing as unknown as grpc.Channel;
    }
  }

  const poolOptions = (channelOptions as any)?.[
    'grpc.spanner_channel_pool_options'
  ] as ChannelPoolOptions | undefined;

  const pool = new ChannelPool(
    address,
    credentials,
    channelOptions,
    poolOptions,
  );

  if (holder) {
    holder.pool = pool;
    if (holder.pools) {
      holder.pools.set(address, pool);
    }
  }
  return pool as unknown as grpc.Channel;
}
