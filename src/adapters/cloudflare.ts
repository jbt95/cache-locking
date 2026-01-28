import { Duration, Effect } from 'effect';
import { AdapterError } from '@core/errors';
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import { quoteIdentifier, readEpochMillis, toMillisClamped } from '@adapters/utils';

/** Options for Cloudflare KV cache adapter. */
export type CloudflareKvCacheOptions<V> = {
  kv: KVNamespace;
  keyPrefix?: string;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

/** Options for Cloudflare D1 cache adapter. */
export type CloudflareD1CacheOptions<V> = {
  db: D1Database;
  tableName?: string;
  keyColumn?: string;
  valueColumn?: string;
  expiresAtColumn?: string;
  keyPrefix?: string;
  clock?: CoreClock;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

/** Options for Cloudflare D1 leases adapter. */
export type CloudflareD1LeasesOptions = {
  db: D1Database;
  tableName?: string;
  keyColumn?: string;
  ownerColumn?: string;
  expiresAtColumn?: string;
  readyColumn?: string;
  keyPrefix?: string;
  clock?: CoreClock;
};

/** Cloudflare KV cache adapter. */
export class CloudflareKvCache<V> implements Cache<V> {
  private readonly kv: KVNamespace;
  private readonly keyPrefix: string;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: CloudflareKvCacheOptions<V>) {
    this.kv = options.kv;
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
        const value = await this.kv.get(this.cacheKey(key));
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
          await this.kv.put(this.cacheKey(key), serialized);
          return;
        }
        const ttlMs = toMillisClamped(ttl);
        const ttlSeconds = Math.max(60, Math.ceil(ttlMs / 1000));
        await this.kv.put(this.cacheKey(key), serialized, { expirationTtl: ttlSeconds });
      },
      catch: (cause) => new AdapterError('cache.set', key, cause),
    });
  }
}

/** Cloudflare D1 cache adapter. */
export class CloudflareD1Cache<V> implements Cache<V> {
  private readonly db: D1Database;
  private readonly tableName: string;
  private readonly keyColumn: string;
  private readonly valueColumn: string;
  private readonly expiresAtColumn: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: CloudflareD1CacheOptions<V>) {
    this.db = options.db;
    this.tableName = quoteIdentifier(options.tableName ?? 'cache');
    this.keyColumn = quoteIdentifier(options.keyColumn ?? 'key');
    this.valueColumn = quoteIdentifier(options.valueColumn ?? 'value');
    this.expiresAtColumn = quoteIdentifier(options.expiresAtColumn ?? 'expires_at');
    this.keyPrefix = options.keyPrefix ?? '';
    this.clock = options.clock ?? { now: () => Date.now() };
    this.serialize = options.serialize ?? ((value) => JSON.stringify(value));
    this.deserialize = options.deserialize ?? ((value) => JSON.parse(value) as V);
  }

  private cacheKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  get(key: string): Effect.Effect<V | null, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const cacheKey = this.cacheKey(key);
        const row = await this.db
          .prepare(
            `SELECT ${this.valueColumn} AS value, ${this.expiresAtColumn} AS expires_at FROM ${this.tableName} WHERE ${this.keyColumn} = ?`,
          )
          .bind(cacheKey)
          .first<{ value: string; expires_at?: number | string | null }>();

        if (!row) {
          return null;
        }

        const expiresAt = readEpochMillis(row.expires_at);
        if (expiresAt !== null && expiresAt <= this.clock.now()) {
          await this.db.prepare(`DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = ?`).bind(cacheKey).run();
          return null;
        }

        if (typeof row.value !== 'string') {
          return null;
        }
        return this.deserialize(row.value);
      },
      catch: (cause) => new AdapterError('cache.get', key, cause),
    });
  }

  set(key: string, value: V, ttl?: Duration.DurationInput): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const cacheKey = this.cacheKey(key);
        const serialized = this.serialize(value);
        const expiresAt = ttl === undefined ? null : this.clock.now() + toMillisClamped(ttl);

        await this.db
          .prepare(
            `INSERT INTO ${this.tableName} (${this.keyColumn}, ${this.valueColumn}, ${this.expiresAtColumn})
             VALUES (?, ?, ?)
             ON CONFLICT (${this.keyColumn}) DO UPDATE SET
               ${this.valueColumn} = excluded.${this.valueColumn},
               ${this.expiresAtColumn} = excluded.${this.expiresAtColumn}`,
          )
          .bind(cacheKey, serialized, expiresAt)
          .run();
      },
      catch: (cause) => new AdapterError('cache.set', key, cause),
    });
  }
}

/** Cloudflare D1 leases adapter. */
export class CloudflareD1Leases implements Leases {
  private readonly db: D1Database;
  private readonly tableName: string;
  private readonly keyColumn: string;
  private readonly ownerColumn: string;
  private readonly expiresAtColumn: string;
  private readonly readyColumn: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;

  constructor(options: CloudflareD1LeasesOptions) {
    this.db = options.db;
    this.tableName = quoteIdentifier(options.tableName ?? 'leases');
    this.keyColumn = quoteIdentifier(options.keyColumn ?? 'key');
    this.ownerColumn = quoteIdentifier(options.ownerColumn ?? 'owner');
    this.expiresAtColumn = quoteIdentifier(options.expiresAtColumn ?? 'expires_at');
    this.readyColumn = quoteIdentifier(options.readyColumn ?? 'ready');
    this.keyPrefix = options.keyPrefix ?? 'lease:';
    this.clock = options.clock ?? { now: () => Date.now() };
  }

  private leaseKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  acquire(key: string, owner: string, ttl: Duration.DurationInput): Effect.Effect<LeaseAcquireResult, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const now = this.clock.now();
        const ttlMs = toMillisClamped(ttl);
        const leaseUntil = now + ttlMs;
        const leaseKey = this.leaseKey(key);

        const result = await this.db
          .prepare(
            `INSERT INTO ${this.tableName} (${this.keyColumn}, ${this.ownerColumn}, ${this.expiresAtColumn}, ${this.readyColumn})
             VALUES (?, ?, ?, 0)
             ON CONFLICT (${this.keyColumn}) DO UPDATE SET
               ${this.ownerColumn} = excluded.${this.ownerColumn},
               ${this.expiresAtColumn} = excluded.${this.expiresAtColumn},
               ${this.readyColumn} = 0
             WHERE ${this.tableName}.${this.expiresAtColumn} <= ?`,
          )
          .bind(leaseKey, owner, leaseUntil, now)
          .run();

        const changes = result.meta.changes ?? 0;
        if (changes > 0) {
          return { role: 'leader', leaseUntil };
        }

        const row = await this.db
          .prepare(`SELECT ${this.expiresAtColumn} AS expires_at FROM ${this.tableName} WHERE ${this.keyColumn} = ?`)
          .bind(leaseKey)
          .first<{ expires_at?: number | string | null }>();
        const expiresAt = row ? readEpochMillis(row.expires_at) : null;
        return { role: 'follower', leaseUntil: expiresAt ?? now };
      },
      catch: (cause) => new AdapterError('leases.acquire', key, cause),
    });
  }

  release(key: string, owner: string): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        await this.db
          .prepare(`DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = ? AND ${this.ownerColumn} = ?`)
          .bind(this.leaseKey(key), owner)
          .run();
      },
      catch: (cause) => new AdapterError('leases.release', key, cause),
    });
  }

  markReady(key: string): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        await this.db
          .prepare(`UPDATE ${this.tableName} SET ${this.readyColumn} = 1 WHERE ${this.keyColumn} = ?`)
          .bind(this.leaseKey(key))
          .run();
      },
      catch: (cause) => new AdapterError('leases.markReady', key, cause),
    });
  }

  isReady(key: string): Effect.Effect<LeaseReadyState, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const leaseKey = this.leaseKey(key);
        const row = await this.db
          .prepare(
            `SELECT ${this.readyColumn} AS ready, ${this.expiresAtColumn} AS expires_at FROM ${this.tableName} WHERE ${this.keyColumn} = ?`,
          )
          .bind(leaseKey)
          .first<{ ready?: number | boolean; expires_at?: number | string | null }>();

        if (!row) {
          return { ready: false, expired: true };
        }

        const expiresAt = readEpochMillis(row.expires_at);
        if (expiresAt !== null && expiresAt <= this.clock.now()) {
          await this.db.prepare(`DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = ?`).bind(leaseKey).run();
          return { ready: false, expired: true };
        }

        return { ready: Boolean(row.ready), expired: false };
      },
      catch: (cause) => new AdapterError('leases.isReady', key, cause),
    });
  }
}

/** Create a Cloudflare KV cache adapter instance. */
export const createCloudflareKvCache = <V>(options: CloudflareKvCacheOptions<V>): Cache<V> =>
  new CloudflareKvCache(options);
/** Create a Cloudflare D1 cache adapter instance. */
export const createCloudflareD1Cache = <V>(options: CloudflareD1CacheOptions<V>): Cache<V> =>
  new CloudflareD1Cache(options);
/** Create a Cloudflare D1 leases adapter instance. */
export const createCloudflareD1Leases = (options: CloudflareD1LeasesOptions): Leases => new CloudflareD1Leases(options);
