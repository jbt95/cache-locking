import { Duration, Effect } from 'effect';
import { AdapterError } from '@core/errors';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import { toMillisClamped } from '@adapters/utils';
import type { RedisClientType } from 'redis';

/** Options for Redis cache adapter. */
export type RedisCacheOptions<V> = {
  client: Pick<RedisClientType, 'get' | 'set'>;
  keyPrefix?: string;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

/** Options for Redis leases adapter. */
export type RedisLeasesOptions = {
  client: Pick<RedisClientType, 'set' | 'get' | 'pTTL' | 'eval'>;
  keyPrefix?: string;
  readyKeyPrefix?: string;
  readyTtl?: Duration.DurationInput;
  clock?: CoreClock;
};

/** Lua script for safe lease release. */
export const REDIS_RELEASE_SCRIPT =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

const DEFAULT_READY_TTL = Duration.seconds(5);

/** Redis cache adapter using string serialization. */
export class RedisCache<V> implements Cache<V> {
  private readonly client: Pick<RedisClientType, 'get' | 'set'>;
  private readonly keyPrefix: string;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: RedisCacheOptions<V>) {
    this.client = options.client;
    this.keyPrefix = options.keyPrefix ?? '';
    this.serialize = options.serialize ?? ((value) => JSON.stringify(value));
    this.deserialize = options.deserialize ?? ((value) => JSON.parse(value) as V);
  }

  private cacheKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  get(key: string): Effect.Effect<V | null, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const value = await this.client.get(this.cacheKey(key));
        if (value === null) {
          return null;
        }
        return this.deserialize(value);
      },
      catch: (cause) => new AdapterError('cache.get', key, cause),
    });
  }

  set(key: string, value: V, ttl?: Duration.DurationInput): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const serialized = this.serialize(value);
        if (ttl === undefined) {
          await this.client.set(this.cacheKey(key), serialized);
          return;
        }
        const ttlMs = toMillisClamped(ttl);
        await this.client.set(this.cacheKey(key), serialized, { PX: ttlMs });
      },
      catch: (cause) => new AdapterError('cache.set', key, cause),
    });
  }
}

/** Redis leases adapter using compare-and-delete release. */
export class RedisLeases implements Leases {
  private readonly client: Pick<RedisClientType, 'set' | 'get' | 'pTTL' | 'eval'>;
  private readonly clock: CoreClock;
  private readonly keyPrefix: string;
  private readonly readyKeyPrefix: string;
  private readonly readyTtl: Duration.DurationInput;

  constructor(options: RedisLeasesOptions) {
    this.client = options.client;
    this.clock = options.clock ?? { now: () => Date.now() };
    this.keyPrefix = options.keyPrefix ?? 'lease:';
    this.readyKeyPrefix = options.readyKeyPrefix ?? `${this.keyPrefix}ready:`;
    this.readyTtl = options.readyTtl ?? DEFAULT_READY_TTL;
  }

  private leaseKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  private readyKey(key: string): string {
    return `${this.readyKeyPrefix}${key}`;
  }

  acquire(key: string, owner: string, ttl: Duration.DurationInput): Effect.Effect<LeaseAcquireResult, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const ttlMs = toMillisClamped(ttl);
        const ok = await this.client.set(this.leaseKey(key), owner, { PX: ttlMs, NX: true });
        if (ok === 'OK') {
          return { role: 'leader', leaseUntil: this.clock.now() + ttlMs };
        }

        const ttlLeft = await this.client.pTTL(this.leaseKey(key));
        const leaseUntil = ttlLeft > 0 ? this.clock.now() + ttlLeft : this.clock.now();
        return { role: 'follower', leaseUntil };
      },
      catch: (cause) => new AdapterError('leases.acquire', key, cause),
    });
  }

  release(key: string, owner: string): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        await this.client.eval(REDIS_RELEASE_SCRIPT, { keys: [this.leaseKey(key)], arguments: [owner] });
      },
      catch: (cause) => new AdapterError('leases.release', key, cause),
    });
  }

  markReady(key: string): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const ttlMs = toMillisClamped(this.readyTtl);
        await this.client.set(this.readyKey(key), '1', { PX: ttlMs });
      },
      catch: (cause) => new AdapterError('leases.markReady', key, cause),
    });
  }

  isReady(key: string): Effect.Effect<LeaseReadyState, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const ready = await this.client.get(this.readyKey(key));
        if (ready) {
          return { ready: true, expired: false };
        }
        const ttlLeft = await this.client.pTTL(this.leaseKey(key));
        return { ready: false, expired: ttlLeft <= 0 };
      },
      catch: (cause) => new AdapterError('leases.isReady', key, cause),
    });
  }
}

/** Create a Redis cache adapter instance. */
export const createRedisCache = <V>(options: RedisCacheOptions<V>): Cache<V> => new RedisCache(options);
/** Create a Redis leases adapter instance. */
export const createRedisLeases = (options: RedisLeasesOptions): Leases => new RedisLeases(options);
