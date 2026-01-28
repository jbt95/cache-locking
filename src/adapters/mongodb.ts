import { Duration, Effect } from 'effect';
import { AdapterError } from '@core/errors';
import type { Cache, CoreClock, LeaseAcquireResult, LeaseReadyState, Leases } from '@core/types';
import { readEpochMillis, toMillisClamped } from '@adapters/utils';
import type { Collection } from 'mongodb';

/** Options for MongoDB cache adapter. */
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

/** Options for MongoDB leases adapter. */
export type MongoLeasesOptions = {
  collection: Collection;
  keyField?: string;
  ownerField?: string;
  expiresAtField?: string;
  readyField?: string;
  keyPrefix?: string;
  clock?: CoreClock;
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

/** MongoDB cache adapter. */
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

  get(key: string): Effect.Effect<V | null, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
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
      },
      catch: (cause) => new AdapterError('cache.get', key, cause),
    });
  }

  set(key: string, value: V, ttl?: Duration.DurationInput): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
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
          const ttlMs = toMillisClamped(ttl);
          const expiresAt = this.clock.now() + ttlMs;
          update.$set[this.expiresAtField] = new Date(expiresAt);
        }

        await this.collection.updateOne({ [this.keyField]: cacheKey }, update, { upsert: true });
      },
      catch: (cause) => new AdapterError('cache.set', key, cause),
    });
  }
}

/** MongoDB leases adapter. */
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

  acquire(key: string, owner: string, ttl: Duration.DurationInput): Effect.Effect<LeaseAcquireResult, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const now = this.clock.now();
        const ttlMs = toMillisClamped(ttl);
        const leaseUntil = now + ttlMs;
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

        const doc = (await this.collection.findOneAndUpdate({ [this.keyField]: leaseKey }, updatePipeline, {
          upsert: true,
          returnDocument: 'before',
        })) as Record<string, unknown> | null;
        if (!doc) {
          return { role: 'leader', leaseUntil };
        }

        const existingUntil = readEpochMillis(doc[this.expiresAtField]);
        if (existingUntil === null || existingUntil <= now) {
          return { role: 'leader', leaseUntil };
        }

        return { role: 'follower', leaseUntil: existingUntil };
      },
      catch: (cause) => new AdapterError('leases.acquire', key, cause),
    });
  }

  release(key: string, owner: string): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        await this.collection.deleteOne({ [this.keyField]: this.leaseKey(key), [this.ownerField]: owner });
      },
      catch: (cause) => new AdapterError('leases.release', key, cause),
    });
  }

  markReady(key: string): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        await this.collection.updateOne({ [this.keyField]: this.leaseKey(key) }, { $set: { [this.readyField]: true } });
      },
      catch: (cause) => new AdapterError('leases.markReady', key, cause),
    });
  }

  isReady(key: string): Effect.Effect<LeaseReadyState, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
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
      },
      catch: (cause) => new AdapterError('leases.isReady', key, cause),
    });
  }
}

/** Create a MongoDB cache adapter instance. */
export const createMongoCache = <V>(options: MongoCacheOptions<V>): Cache<V> => new MongoCache(options);
/** Create a MongoDB leases adapter instance. */
export const createMongoLeases = (options: MongoLeasesOptions): Leases => new MongoLeases(options);
