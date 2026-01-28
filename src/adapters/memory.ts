import { Duration, Effect } from 'effect';
import { AdapterError } from '@core/errors';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import { toMillisClamped } from '@adapters/utils';

type CacheEntry<V> = {
  value: V;
  expiresAt?: number;
};

type LeaseEntry = {
  holder: string;
  expiresAt: number;
  ready: boolean;
};

/** Options for in-memory cache adapter. */
export type MemoryCacheOptions = {
  clock?: CoreClock;
};

/** Options for in-memory lease adapter. */
export type MemoryLeasesOptions = {
  clock?: CoreClock;
};

/** In-memory cache adapter for tests and local usage. */
export class MemoryCache<V> implements Cache<V> {
  private readonly store = new Map<string, CacheEntry<V>>();
  private readonly clock: CoreClock;

  constructor(options?: MemoryCacheOptions) {
    this.clock = options?.clock ?? { now: () => Date.now() };
  }

  get(key: string): Effect.Effect<V | null, AdapterError> {
    return Effect.try({
      try: () => {
        const entry = this.store.get(key);
        if (!entry) {
          return null;
        }
        if (entry.expiresAt !== undefined && entry.expiresAt <= this.clock.now()) {
          this.store.delete(key);
          return null;
        }
        return entry.value;
      },
      catch: (cause) => new AdapterError('cache.get', key, cause),
    });
  }

  set(key: string, value: V, ttl?: Duration.DurationInput): Effect.Effect<void, AdapterError> {
    return Effect.try({
      try: () => {
        const ttlMs = ttl === undefined ? undefined : toMillisClamped(ttl);
        const expiresAt = ttlMs === undefined ? undefined : this.clock.now() + ttlMs;
        this.store.set(key, { value, expiresAt });
      },
      catch: (cause) => new AdapterError('cache.set', key, cause),
    });
  }
}

/** In-memory lease adapter for tests and local usage. */
export class MemoryLeases implements Leases {
  private readonly leases = new Map<string, LeaseEntry>();
  private readonly clock: CoreClock;

  constructor(options?: MemoryLeasesOptions) {
    this.clock = options?.clock ?? { now: () => Date.now() };
  }

  acquire(key: string, owner: string, ttl: Duration.DurationInput): Effect.Effect<LeaseAcquireResult, AdapterError> {
    return Effect.try({
      try: () => {
        const now = this.clock.now();
        const entry = this.leases.get(key);
        if (!entry || entry.expiresAt <= now) {
          const ttlMs = toMillisClamped(ttl);
          const expiresAt = now + ttlMs;
          this.leases.set(key, { holder: owner, expiresAt, ready: false });
          return { role: 'leader', leaseUntil: expiresAt };
        }
        return { role: 'follower', leaseUntil: entry.expiresAt };
      },
      catch: (cause) => new AdapterError('leases.acquire', key, cause),
    });
  }

  release(key: string, owner: string): Effect.Effect<void, AdapterError> {
    return Effect.try({
      try: () => {
        const entry = this.leases.get(key);
        if (entry && entry.holder === owner) {
          this.leases.delete(key);
        }
      },
      catch: (cause) => new AdapterError('leases.release', key, cause),
    });
  }

  markReady(key: string): Effect.Effect<void, AdapterError> {
    return Effect.try({
      try: () => {
        const entry = this.leases.get(key);
        if (entry) {
          entry.ready = true;
        }
      },
      catch: (cause) => new AdapterError('leases.markReady', key, cause),
    });
  }

  isReady(key: string): Effect.Effect<LeaseReadyState, AdapterError> {
    return Effect.try({
      try: () => {
        const entry = this.leases.get(key);
        if (!entry) {
          return { ready: false, expired: true };
        }

        if (entry.expiresAt <= this.clock.now()) {
          this.leases.delete(key);
          return { ready: false, expired: true };
        }

        return { ready: entry.ready, expired: false };
      },
      catch: (cause) => new AdapterError('leases.isReady', key, cause),
    });
  }
}
