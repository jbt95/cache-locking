import { Duration } from 'effect';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';

type CacheEntry<V> = {
  value: V;
  expiresAt?: number;
};

type LeaseEntry = {
  holder: string;
  expiresAt: number;
  ready: boolean;
};

export type MemoryCacheOptions = {
  clock?: CoreClock;
};

export type MemoryLeasesOptions = {
  clock?: CoreClock;
};

export class MemoryCache<V> implements Cache<V> {
  private readonly store = new Map<string, CacheEntry<V>>();
  private readonly clock: CoreClock;

  constructor(options?: MemoryCacheOptions) {
    this.clock = options?.clock ?? { now: () => Date.now() };
  }

  async get(key: string): Promise<V | null> {
    const entry = this.store.get(key);
    if (!entry) {
      return null;
    }
    if (entry.expiresAt !== undefined && entry.expiresAt <= this.clock.now()) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const ttlMs = ttl === undefined ? undefined : Duration.toMillis(ttl);
    const expiresAt = Number.isFinite(ttlMs ?? NaN) ? this.clock.now() + Math.max(0, ttlMs ?? 0) : undefined;
    this.store.set(key, { value, expiresAt });
  }
}

export class MemoryLeases implements Leases {
  private readonly leases = new Map<string, LeaseEntry>();
  private readonly clock: CoreClock;

  constructor(options?: MemoryLeasesOptions) {
    this.clock = options?.clock ?? { now: () => Date.now() };
  }

  async acquire(key: string, owner: string, ttl: Duration.DurationInput): Promise<LeaseAcquireResult> {
    const now = this.clock.now();
    const entry = this.leases.get(key);
    if (!entry || entry.expiresAt <= now) {
      const ttlMs = Duration.toMillis(ttl);
      const expiresAt = now + Math.max(0, ttlMs);
      this.leases.set(key, { holder: owner, expiresAt, ready: false });
      return { role: 'leader', leaseUntil: expiresAt };
    }
    return { role: 'follower', leaseUntil: entry.expiresAt };
  }

  async release(key: string, owner: string): Promise<void> {
    const entry = this.leases.get(key);
    if (entry && entry.holder === owner) {
      this.leases.delete(key);
    }
  }

  async markReady(key: string): Promise<void> {
    const entry = this.leases.get(key);
    if (entry) {
      entry.ready = true;
    }
  }

  async isReady(key: string): Promise<LeaseReadyState> {
    const entry = this.leases.get(key);
    if (!entry) {
      return { ready: false, expired: true };
    }

    if (entry.expiresAt <= this.clock.now()) {
      this.leases.delete(key);
      return { ready: false, expired: true };
    }

    return { ready: entry.ready, expired: false };
  }
}
