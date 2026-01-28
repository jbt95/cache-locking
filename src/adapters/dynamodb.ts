import { Duration, Effect } from 'effect';
import { AdapterError } from '@core/errors';
import type { Cache, CoreClock } from '@core/types';
import { toMillisClamped } from '@adapters/utils';
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

/** Options for DynamoDB cache adapter. */
export type DynamoDbCacheOptions<V> = {
  client: Pick<DynamoDBDocumentClient, 'send'>;
  tableName: string;
  keyAttribute?: string;
  valueAttribute?: string;
  ttlAttribute?: string;
  keyPrefix?: string;
  clock?: CoreClock;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

/** DynamoDB cache adapter with TTL attribute support. */
export class DynamoDbCache<V> implements Cache<V> {
  private readonly client: Pick<DynamoDBDocumentClient, 'send'>;
  private readonly tableName: string;
  private readonly keyAttribute: string;
  private readonly valueAttribute: string;
  private readonly ttlAttribute?: string;
  private readonly keyPrefix: string;
  private readonly clock: CoreClock;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: DynamoDbCacheOptions<V>) {
    this.client = options.client;
    this.tableName = options.tableName;
    this.keyAttribute = options.keyAttribute ?? 'key';
    this.valueAttribute = options.valueAttribute ?? 'value';
    this.ttlAttribute = options.ttlAttribute ?? 'ttl';
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
        const response = await this.client.send(
          new GetCommand({
            TableName: this.tableName,
            Key: { [this.keyAttribute]: this.cacheKey(key) },
          }),
        );
        const item = response.Item as Record<string, unknown> | undefined;
        if (!item) {
          return null;
        }
        const stored = item[this.valueAttribute];
        if (typeof stored !== 'string') {
          return null;
        }
        return this.deserialize(stored);
      },
      catch: (cause) => new AdapterError('cache.get', key, cause),
    });
  }

  set(key: string, value: V, ttl?: Duration.DurationInput): Effect.Effect<void, AdapterError> {
    return Effect.tryPromise({
      try: async () => {
        const item: Record<string, unknown> = {
          [this.keyAttribute]: this.cacheKey(key),
          [this.valueAttribute]: this.serialize(value),
        };

        if (ttl !== undefined && this.ttlAttribute) {
          const ttlMs = toMillisClamped(ttl);
          const expiresAtMs = this.clock.now() + ttlMs;
          item[this.ttlAttribute] = Math.ceil(expiresAtMs / 1000);
        }

        await this.client.send(
          new PutCommand({
            TableName: this.tableName,
            Item: item,
          }),
        );
      },
      catch: (cause) => new AdapterError('cache.set', key, cause),
    });
  }
}

/** Create a DynamoDB cache adapter instance. */
export const createDynamoDbCache = <V>(options: DynamoDbCacheOptions<V>): Cache<V> => new DynamoDbCache(options);
