import { Duration } from 'effect';
import type { Cache, CoreClock } from '@core/types';
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

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

  async get(key: string): Promise<V | null> {
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
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const item: Record<string, unknown> = {
      [this.keyAttribute]: this.cacheKey(key),
      [this.valueAttribute]: this.serialize(value),
    };

    if (ttl !== undefined && this.ttlAttribute) {
      const ttlMs = Duration.toMillis(ttl);
      const expiresAtMs = this.clock.now() + Math.max(0, ttlMs);
      item[this.ttlAttribute] = Math.ceil(expiresAtMs / 1000);
    }

    await this.client.send(
      new PutCommand({
        TableName: this.tableName,
        Item: item,
      }),
    );
  }
}

export const createDynamoDbCache = <V>(options: DynamoDbCacheOptions<V>): Cache<V> => new DynamoDbCache(options);
