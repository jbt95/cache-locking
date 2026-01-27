import { Duration } from 'effect';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import type { Collection } from 'mongodb';

export type MongoCacheOptions<V> = {
  collection: Collection;
  keyField?: string;
  valueField?: string;
  expiresAtField?: string;
  keyPrefix?: string;
  clock?: CoreClock;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

export type MongoLeasesOptions = {
  collection: Collection;
  keyField?: string;
  ownerField?: string;
  expiresAtField?: string;
  readyField?: string;
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

const normalizeStoredValue = (value: unknown): string | null => {
  if (typeof value === 'string') {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString();
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString();
  }
  if (value && typeof value === 'object' && 'buffer' in value) {
    const bufferValue = (value as { buffer?: unknown; position?: number }).buffer;
    if (bufferValue instanceof Uint8Array) {
      const length = (value as { position?: number }).position;
      const slice = typeof length === 'number' ? bufferValue.subarray(0, length) : bufferValue;
      return Buffer.from(slice).toString();
    }
  }
  return null;
};

export class MongoCache<V> implements Cache<V> {
  private readonly collection: Collection;
  private readonly keyField: string;
  private readonly valueField: string;
  private readonly expiresAtField: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: MongoCacheOptions<V>) {
    this.collection = options.collection;
    this.keyField = options.keyField ?? 'key';
    this.valueField = options.valueField ?? 'value';
    this.expiresAtField = options.expiresAtField ?? 'expiresAt';
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
    const doc = (await this.collection.findOne({ [this.keyField]: cacheKey })) as Record<string, unknown> | null;
    if (!doc) {
      return null;
    }

    const expiresAt = readEpochMillis(doc[this.expiresAtField]);
    if (expiresAt !== null && expiresAt <= this.clock.now()) {
      await this.collection.deleteOne({ [this.keyField]: cacheKey });
      return null;
    }

    const stored = doc[this.valueField];
    const storedValue = normalizeStoredValue(stored);
    if (storedValue === null) {
      return null;
    }
    return this.deserialize(storedValue);
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const cacheKey = this.cacheKey(key);
    const serialized = this.serialize(value);
    const update: Record<string, Record<string, unknown>> = {
      $set: {
        [this.valueField]: serialized,
      },
      $setOnInsert: {
        [this.keyField]: cacheKey,
      },
    };

    if (ttl === undefined) {
      update.$unset = { [this.expiresAtField]: '' };
    } else {
      const ttlMs = Duration.toMillis(ttl);
      const expiresAt = this.clock.now() + Math.max(0, ttlMs);
      update.$set[this.expiresAtField] = new Date(expiresAt);
    }

    await this.collection.updateOne({ [this.keyField]: cacheKey }, update, { upsert: true });
  }
}

export class MongoLeases implements Leases {
  private readonly collection: Collection;
  private readonly keyField: string;
  private readonly ownerField: string;
  private readonly expiresAtField: string;
  private readonly readyField: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;

  constructor(options: MongoLeasesOptions) {
    this.collection = options.collection;
    this.keyField = options.keyField ?? 'key';
    this.ownerField = options.ownerField ?? 'owner';
    this.expiresAtField = options.expiresAtField ?? 'expiresAt';
    this.readyField = options.readyField ?? 'ready';
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

    const nowDate = new Date(now);
    const leaseUntilDate = new Date(leaseUntil);
    const expiresAtPath = `$${this.expiresAtField}`;
    const ownerPath = `$${this.ownerField}`;
    const readyPath = `$${this.readyField}`;

    const expiredCondition = {
      $or: [{ $eq: [expiresAtPath, null] }, { $lte: [expiresAtPath, nowDate] }],
    };

    const updatePipeline = [
      {
        $set: {
          [this.keyField]: leaseKey,
          [this.ownerField]: { $cond: [expiredCondition, owner, ownerPath] },
          [this.expiresAtField]: { $cond: [expiredCondition, leaseUntilDate, expiresAtPath] },
          [this.readyField]: { $cond: [expiredCondition, false, readyPath] },
        },
      },
    ];

    const doc = (await this.collection.findOneAndUpdate(
      { [this.keyField]: leaseKey },
      updatePipeline,
      { upsert: true, returnDocument: 'before' },
    )) as Record<string, unknown> | null;
    if (!doc) {
      return { role: 'leader', leaseUntil };
    }

    const existingUntil = readEpochMillis(doc[this.expiresAtField]);
    if (existingUntil === null || existingUntil <= now) {
      return { role: 'leader', leaseUntil };
    }

    return { role: 'follower', leaseUntil: existingUntil };
  }

  async release(key: string, owner: string): Promise<void> {
    await this.collection.deleteOne({ [this.keyField]: this.leaseKey(key), [this.ownerField]: owner });
  }

  async markReady(key: string): Promise<void> {
    await this.collection.updateOne({ [this.keyField]: this.leaseKey(key) }, { $set: { [this.readyField]: true } });
  }

  async isReady(key: string): Promise<LeaseReadyState> {
    const leaseKey = this.leaseKey(key);
    const doc = (await this.collection.findOne({ [this.keyField]: leaseKey })) as Record<string, unknown> | null;
    if (!doc) {
      return { ready: false, expired: true };
    }

    const expiresAt = readEpochMillis(doc[this.expiresAtField]);
    if (expiresAt !== null && expiresAt <= this.clock.now()) {
      await this.collection.deleteOne({ [this.keyField]: leaseKey });
      return { ready: false, expired: true };
    }

    return { ready: Boolean(doc[this.readyField]), expired: false };
  }
}

export const createMongoCache = <V>(options: MongoCacheOptions<V>): Cache<V> => new MongoCache(options);
export const createMongoLeases = (options: MongoLeasesOptions): Leases => new MongoLeases(options);
