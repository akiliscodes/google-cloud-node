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

import {ChannelLease, RwTransactionAffinityGuard} from './entry';

/**
 * Stickiness kind for transaction channel affinity.
 */
export enum AffinityKind {
  /**
   * Read/Write transactions require hard stickiness to ensure all statements
   * route to the same SpanFE, even if the channel transitions to draining.
   */
  ReadWrite = 'READ_WRITE',

  /**
   * Read-Only transactions prefer soft stickiness for cache warmth, but seamlessly
   * switch to a fresh active channel if their pinned channel begins draining.
   */
  ReadOnly = 'READ_ONLY',
}

/**
 * Result of an atomic compare-and-set operation on a transaction affinity handle.
 */
export interface CasResult {
  success: boolean;
  winnerId: number;
}

/**
 * Caller-owned handle managing channel affinity across multi-statement transactions.
 *
 * Pins statements within a transaction to the same physical gRPC channel, tracks
 * active Read/Write transaction count on the channel, and provides lock-free
 * CAS conflict resolution for concurrent asynchronous statements.
 */
export class TransactionAffinity {
  private entryId = 0;
  private readonly kind: AffinityKind;
  private readWriteGuard?: RwTransactionAffinityGuard;
  private disposed = false;

  constructor(kind: AffinityKind = AffinityKind.ReadWrite) {
    this.kind = kind;
  }

  /**
   * Creates a new unpinned handle for Read/Write transactions (hard stickiness).
   */
  static newReadWrite(): TransactionAffinity {
    return new TransactionAffinity(AffinityKind.ReadWrite);
  }

  /**
   * Creates a new unpinned handle for Read-Only transactions (soft stickiness).
   */
  static newReadOnly(): TransactionAffinity {
    return new TransactionAffinity(AffinityKind.ReadOnly);
  }

  /**
   * Returns true if this handle requires hard stickiness (Read/Write transactions).
   */
  isReadWrite(): boolean {
    return this.kind === AffinityKind.ReadWrite;
  }

  /**
   * Returns true if this handle uses soft stickiness (Read-Only transactions).
   */
  isReadOnly(): boolean {
    return this.kind === AffinityKind.ReadOnly;
  }

  /**
   * Returns the pinned monotonic channel entry ID, or null if unpinned.
   */
  pinnedEntryId(): number | null {
    return this.entryId !== 0 ? this.entryId : null;
  }

  /**
   * Sets the pinned channel entry ID directly.
   *
   * @param entryId Non-zero positive monotonic channel entry ID.
   */
  setEntryId(entryId: number): void {
    if (!Number.isInteger(entryId) || entryId <= 0) {
      throw new Error('entryId must be a positive non-zero integer');
    }
    this.entryId = entryId;
  }

  /**
   * Attempts to atomically update the pinned entry ID from current to next.
   *
   * @param current The expected current entry ID (or 0 if unpinned).
   * @param next The new entry ID to pin.
   * @returns CasResult indicating success and the winning entry ID.
   */
  compareAndSetEntryId(current: number, next: number): CasResult {
    if (this.entryId === current) {
      this.entryId = next;
      return {success: true, winnerId: next};
    }
    return {success: false, winnerId: this.entryId};
  }

  /**
   * Ensures that an active Read/Write transaction guard is attached to this handle
   * for the leased channel entry, incrementing active_rw_transactions on the channel.
   *
   * If already pinned to the same channel entry, this operation is an idempotent no-op.
   * If re-pinning to a different channel entry, the previous guard is released and
   * a new guard is acquired.
   *
   * @param lease The acquired channel lease.
   */
  ensureReadWriteGuard(lease: ChannelLease): void {
    if (!this.isReadWrite() || this.disposed) {
      return;
    }
    if (this.readWriteGuard && this.readWriteGuard.entryId === lease.entryId) {
      return;
    }
    if (this.readWriteGuard) {
      this.readWriteGuard.dispose();
      this.readWriteGuard = undefined;
    }
    this.readWriteGuard = lease.rwAffinityGuard();
  }

  ensureRwGuard(lease: ChannelLease): void {
    this.ensureReadWriteGuard(lease);
  }

  hasReadWriteGuard(): boolean {
    return this.readWriteGuard !== undefined;
  }

  hasRwGuard(): boolean {
    return this.hasReadWriteGuard();
  }

  /**
   * Clears the pinned entry ID and releases any attached Read/Write guard.
   */
  reset(): void {
    this.entryId = 0;
    if (this.readWriteGuard) {
      this.readWriteGuard.dispose();
      this.readWriteGuard = undefined;
    }
  }

  /**
   * Releases and disposes the affinity handle upon transaction completion (commit, rollback, or destruction).
   */
  release(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.reset();
  }
}
