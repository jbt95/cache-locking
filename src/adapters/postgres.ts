import { Duration } from 'effect';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import type { ClientBase, Pool, QueryResult } from 'pg';

export type PostgresClient = Pool | ClientBase;

export type PostgresCacheOptions<V> = {
  client: PostgresClient;
  tableName: string;
  keyColumn?: string;
  valueColumn?: string;
  expiresAtColumn?: string;
  keyPrefix?: string;
  clock?: CoreClock;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

export type PostgresLeasesOptions = {
  client: PostgresClient;
  tableName: string;
  keyColumn?: string;
  ownerColumn?: string;
  expiresAtColumn?: string;
  readyColumn?: string;
  keyPrefix?: string;
  clock?: CoreClock;
};

const quoteIdentifier = (value: string): string => `"${value.replace(/"/g, '""')}"`;

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

export class PostgresCache<V> implements Cache<V> {
  private readonly client: PostgresClient;
  private readonly tableName: string;
  private readonly keyColumn: string;
  private readonly valueColumn: string;
  private readonly expiresAtColumn: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: PostgresCacheOptions<V>) {
    this.client = options.client;
    this.tableName = quoteIdentifier(options.tableName);
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
    const result = (await this.client.query(
      `SELECT ${this.valueColumn} AS value, ${this.expiresAtColumn} AS expires_at FROM ${this.tableName} WHERE ${this.keyColumn} = $1`,
      [cacheKey],
    )) as QueryResult<Record<string, unknown>>;

    const row = result.rows[0];
    if (!row) {
      return null;
    }

    const expiresAt = readEpochMillis(row.expires_at);
    if (expiresAt !== null && expiresAt <= this.clock.now()) {
      await this.client.query(
        `DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = $1`,
        [cacheKey],
      );
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
      ttl === undefined ? null : new Date(this.clock.now() + Math.max(0, Duration.toMillis(ttl)));

    await this.client.query(
      `INSERT INTO ${this.tableName} (${this.keyColumn}, ${this.valueColumn}, ${this.expiresAtColumn})
       VALUES ($1, $2, $3)
       ON CONFLICT (${this.keyColumn}) DO UPDATE SET
         ${this.valueColumn} = EXCLUDED.${this.valueColumn},
         ${this.expiresAtColumn} = EXCLUDED.${this.expiresAtColumn}`,
      [cacheKey, serialized, expiresAt],
    );
  }
}

export class PostgresLeases implements Leases {
  private readonly client: PostgresClient;
  private readonly tableName: string;
  private readonly keyColumn: string;
  private readonly ownerColumn: string;
  private readonly expiresAtColumn: string;
  private readonly readyColumn: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;

  constructor(options: PostgresLeasesOptions) {
    this.client = options.client;
    this.tableName = quoteIdentifier(options.tableName);
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
    const expiresAt = new Date(leaseUntil);

    const insertResult = (await this.client.query(
      `INSERT INTO ${this.tableName} (${this.keyColumn}, ${this.ownerColumn}, ${this.expiresAtColumn}, ${this.readyColumn})
       VALUES ($1, $2, $3, false)
       ON CONFLICT (${this.keyColumn}) DO UPDATE SET
         ${this.ownerColumn} = EXCLUDED.${this.ownerColumn},
         ${this.expiresAtColumn} = EXCLUDED.${this.expiresAtColumn},
         ${this.readyColumn} = false
       WHERE ${this.tableName}.${this.expiresAtColumn} <= $4
       RETURNING ${this.expiresAtColumn} AS expires_at`,
      [leaseKey, owner, expiresAt, new Date(now)],
    )) as QueryResult<Record<string, unknown>>;

    if (insertResult.rowCount && insertResult.rowCount > 0) {
      return { role: 'leader', leaseUntil };
    }

    const current = (await this.client.query(
      `SELECT ${this.expiresAtColumn} AS expires_at FROM ${this.tableName} WHERE ${this.keyColumn} = $1`,
      [leaseKey],
    )) as QueryResult<Record<string, unknown>>;
    const expiresAtValue = readEpochMillis(current.rows[0]?.expires_at);
    return { role: 'follower', leaseUntil: expiresAtValue ?? now };
  }

  async release(key: string, owner: string): Promise<void> {
    await this.client.query(
      `DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = $1 AND ${this.ownerColumn} = $2`,
      [this.leaseKey(key), owner],
    );
  }

  async markReady(key: string): Promise<void> {
    await this.client.query(
      `UPDATE ${this.tableName} SET ${this.readyColumn} = true WHERE ${this.keyColumn} = $1`,
      [this.leaseKey(key)],
    );
  }

  async isReady(key: string): Promise<LeaseReadyState> {
    const leaseKey = this.leaseKey(key);
    const result = (await this.client.query(
      `SELECT ${this.readyColumn} AS ready, ${this.expiresAtColumn} AS expires_at FROM ${this.tableName} WHERE ${this.keyColumn} = $1`,
      [leaseKey],
    )) as QueryResult<Record<string, unknown>>;

    const row = result.rows[0];
    if (!row) {
      return { ready: false, expired: true };
    }

    const expiresAt = readEpochMillis(row.expires_at);
    if (expiresAt !== null && expiresAt <= this.clock.now()) {
      await this.client.query(
        `DELETE FROM ${this.tableName} WHERE ${this.keyColumn} = $1`,
        [leaseKey],
      );
      return { ready: false, expired: true };
    }

    return { ready: Boolean(row.ready), expired: false };
  }
}

export const createPostgresCache = <V>(options: PostgresCacheOptions<V>): Cache<V> => new PostgresCache(options);
export const createPostgresLeases = (options: PostgresLeasesOptions): Leases => new PostgresLeases(options);
