import { Duration } from 'effect';
import type { Cache } from '@core/types';
import type { Client as MemjsClient } from 'memjs';

type MemjsValue = string | Buffer | null;
type MemjsClientType = MemjsClient<string | Buffer, MemjsValue>;
type MemjsGetResult = { value: MemjsValue; flags: Buffer | null };

export type MemcachedCacheOptions<V> = {
  client: Pick<MemjsClientType, 'get' | 'set'>;
  keyPrefix?: string;
  serialize?: (value: V) => string;
  deserialize?: (value: string) => V;
};

export class MemcachedCache<V> implements Cache<V> {
  private readonly client: Pick<MemjsClientType, 'get' | 'set'>;
  private readonly keyPrefix: string;
  private readonly serialize: (value: V) => string;
  private readonly deserialize: (value: string) => V;

  constructor(options: MemcachedCacheOptions<V>) {
    this.client = options.client;
    this.keyPrefix = options.keyPrefix ?? '';
    this.serialize = options.serialize ?? ((value) => JSON.stringify(value));
    this.deserialize = options.deserialize ?? ((value) => JSON.parse(value) as V);
  }

  private cacheKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  private extractValue(result: MemjsGetResult | null | undefined): string | Buffer | null {
    if (!result) {
      return null;
    }
    if (typeof result.value === 'string' || Buffer.isBuffer(result.value)) {
      return result.value;
    }
    return null;
  }

  async get(key: string): Promise<V | null> {
    const result = await this.client.get(this.cacheKey(key));
    const value = this.extractValue(result);
    if (value === null) {
      return null;
    }
    const asString = typeof value === 'string' ? value : value.toString();
    return this.deserialize(asString);
  }

  async set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void> {
    const serialized = this.serialize(value);
    if (ttl === undefined) {
      await this.client.set(this.cacheKey(key), serialized);
      return;
    }
    const ttlMs = Duration.toMillis(ttl);
    const ttlSeconds = Math.max(0, Math.ceil(ttlMs / 1000));
    await this.client.set(this.cacheKey(key), serialized, { expires: ttlSeconds });
  }
}

export const createMemcachedCache = <V>(options: MemcachedCacheOptions<V>): Cache<V> => new MemcachedCache(options);
