import { Duration } from 'effect';
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';

export type CloudflareKvCacheOptions<V> = {
  kv: KVNamespace;
  keyPrefix?: string;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

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

const readEpochMillis = (value: unknown): number | null => {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      return asNumber;
    }
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
};

const quoteIdentifier = (value: string): string => `"${value.replace(/"/g, '""')}"`;

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

  async get(key: string): Promise<V | null> {
    const value = await this.kv.get(this.cacheKey(key));
    if (value === null) {
      return null;
    }
    return this.deserialize(value);
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const serialized = this.serialize(value);
    if (ttl === undefined) {
      await this.kv.put(this.cacheKey(key), serialized);
      return;
    }
    const ttlMs = Math.max(0, Duration.toMillis(ttl));
    const ttlSeconds = Math.max(60, Math.ceil(ttlMs / 1000));
    await this.kv.put(this.cacheKey(key), serialized, { expirationTtl: ttlSeconds });
  }
}

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

  async get(key: string): Promise<V | null> {
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
      await this.db
        .prepare(`DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = ?`)
        .bind(cacheKey)
        .run();
      return null;
    }

    if (typeof row.value !== 'string') {
      return null;
    }
    return this.deserialize(row.value);
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const cacheKey = this.cacheKey(key);
    const serialized = this.serialize(value);
    const expiresAt =
      ttl === undefined ? null : this.clock.now() + Math.max(0, Duration.toMillis(ttl));

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
  }
}

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

  async acquire(key: string, owner: string, ttl: Duration.DurationInput): Promise<LeaseAcquireResult> {
    const now = this.clock.now();
    const ttlMs = Duration.toMillis(ttl);
    const leaseUntil = now + Math.max(0, ttlMs);
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
  }

  async release(key: string, owner: string): Promise<void> {
    await this.db
      .prepare(
        `DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = ? AND ${this.ownerColumn} = ?`,
      )
      .bind(this.leaseKey(key), owner)
      .run();
  }

  async markReady(key: string): Promise<void> {
    await this.db
      .prepare(`UPDATE ${this.tableName} SET ${this.readyColumn} = 1 WHERE ${this.keyColumn} = ?`)
      .bind(this.leaseKey(key))
      .run();
  }

  async isReady(key: string): Promise<LeaseReadyState> {
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
      await this.db
        .prepare(`DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = ?`)
        .bind(leaseKey)
        .run();
      return { ready: false, expired: true };
    }

    return { ready: Boolean(row.ready), expired: false };
  }
}

export const createCloudflareKvCache = <V>(options: CloudflareKvCacheOptions<V>): Cache<V> =>
  new CloudflareKvCache(options);
export const createCloudflareD1Cache = <V>(options: CloudflareD1CacheOptions<V>): Cache<V> =>
  new CloudflareD1Cache(options);
export const createCloudflareD1Leases = (options: CloudflareD1LeasesOptions): Leases => new CloudflareD1Leases(options);
