import { Duration } from 'effect';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import type { RedisClientType } from 'redis';

export type RedisCacheOptions<V> = {
  client: Pick<RedisClientType, 'get' | 'set'>;
  keyPrefix?: string;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

export type RedisLeasesOptions = {
  client: Pick<RedisClientType, 'set' | 'get' | 'pTTL' | 'eval'>;
  keyPrefix?: string;
  readyKeyPrefix?: string;
  readyTtl?: Duration.DurationInput;
  clock?: CoreClock;
};

export const REDIS_RELEASE_SCRIPT =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

const DEFAULT_READY_TTL = Duration.seconds(5);

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

  async get(key: string): Promise<V | null> {
    const value = await this.client.get(this.cacheKey(key));
    if (value === null) {
      return null;
    }
    return this.deserialize(value);
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const serialized = this.serialize(value);
    if (ttl === undefined) {
      await this.client.set(this.cacheKey(key), serialized);
      return;
    }
    const ttlMs = Duration.toMillis(ttl);
    await this.client.set(this.cacheKey(key), serialized, { PX: Math.max(0, ttlMs) });
  }
}

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

  async acquire(key: string, owner: string, ttl: Duration.DurationInput): Promise<LeaseAcquireResult> {
    const ttlMs = Duration.toMillis(ttl);
    const ok = await this.client.set(this.leaseKey(key), owner, { PX: Math.max(0, ttlMs), NX: true });
    if (ok === 'OK') {
      return { role: 'leader', leaseUntil: this.clock.now() + Math.max(0, ttlMs) };
    }

    const ttlLeft = await this.client.pTTL(this.leaseKey(key));
    const leaseUntil = ttlLeft > 0 ? this.clock.now() + ttlLeft : this.clock.now();
    return { role: 'follower', leaseUntil };
  }

  async release(key: string, owner: string): Promise<void> {
    await this.client.eval(REDIS_RELEASE_SCRIPT, { keys: [this.leaseKey(key)], arguments: [owner] });
  }

  async markReady(key: string): Promise<void> {
    const ttlMs = Duration.toMillis(this.readyTtl);
    await this.client.set(this.readyKey(key), '1', { PX: Math.max(0, ttlMs) });
  }

  async isReady(key: string): Promise<LeaseReadyState> {
    const ready = await this.client.get(this.readyKey(key));
    if (ready) {
      return { ready: true, expired: false };
    }
    const ttlLeft = await this.client.pTTL(this.leaseKey(key));
    return { ready: false, expired: ttlLeft <= 0 };
  }
}

export const createRedisCache = <V>(options: RedisCacheOptions<V>): Cache<V> => new RedisCache(options);
export const createRedisLeases = (options: RedisLeasesOptions): Leases => new RedisLeases(options);
