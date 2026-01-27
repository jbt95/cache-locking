import type { Cache, Leases } from '@core/types';
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import type { RedisClientType } from 'redis';
import {
  CloudflareD1Cache,
  CloudflareD1Leases,
  CloudflareKvCache,
  type CloudflareD1CacheOptions,
  type CloudflareD1LeasesOptions,
  type CloudflareKvCacheOptions,
} from '@adapters/cloudflare';
import { DynamoDbCache, type DynamoDbCacheOptions } from '@adapters/dynamodb';
import { MemcachedCache, type MemcachedCacheOptions } from '@adapters/memcached';
import { MemoryCache, type MemoryCacheOptions, MemoryLeases, type MemoryLeasesOptions } from '@adapters/memory';
import { MongoCache, MongoLeases, type MongoCacheOptions, type MongoLeasesOptions } from '@adapters/mongodb';
import { PostgresCache, PostgresLeases, type PostgresCacheOptions, type PostgresLeasesOptions } from '@adapters/postgres';
import { R2Cache, R2Leases, type R2CacheOptions, type R2LeasesOptions } from '@adapters/r2';
import { RedisCache, type RedisCacheOptions, RedisLeases, type RedisLeasesOptions } from '@adapters/redis';
import { S3Cache, S3Leases, type S3CacheOptions, type S3LeasesOptions } from '@adapters/s3';

export type ProviderAdapter<V> = {
  cache: Cache<V>;
  leases?: Leases;
};

export type MemoryAdapterOptions = {
  cache?: MemoryCacheOptions;
  leases?: MemoryLeasesOptions;
};

export type RedisAdapterClient = Pick<RedisClientType, 'get' | 'set' | 'pTTL' | 'eval'>;

export type RedisAdapterOptions<V> = {
  client: RedisAdapterClient;
  cache?: Omit<RedisCacheOptions<V>, 'client'>;
  leases?: Omit<RedisLeasesOptions, 'client'>;
};

export type MemcachedAdapterOptions<V> = MemcachedCacheOptions<V>;
export type DynamoDbAdapterOptions<V> = DynamoDbCacheOptions<V>;
export type MongoAdapterOptions<V> = {
  cache: MongoCacheOptions<V>;
  leases: MongoLeasesOptions;
};
export type PostgresAdapterOptions<V> = {
  cache: PostgresCacheOptions<V>;
  leases: PostgresLeasesOptions;
};
export type S3AdapterOptions<V> = {
  cache: S3CacheOptions<V>;
  leases: S3LeasesOptions;
};
export type R2AdapterOptions<V> = {
  cache: R2CacheOptions<V>;
  leases: R2LeasesOptions;
};
export type CloudflareKvAdapterOptions<V> = {
  kv: KVNamespace;
  leasesDb: D1Database;
  cache?: Omit<CloudflareKvCacheOptions<V>, 'kv'>;
  leases?: Omit<CloudflareD1LeasesOptions, 'db'>;
};
export type CloudflareD1AdapterOptions<V> = {
  db: D1Database;
  cache?: Omit<CloudflareD1CacheOptions<V>, 'db'>;
  leases?: Omit<CloudflareD1LeasesOptions, 'db'>;
};

export class MemoryAdapter<V> implements ProviderAdapter<V> {
  cache: MemoryCache<V>;
  leases: MemoryLeases;

  constructor(options?: MemoryAdapterOptions) {
    this.cache = new MemoryCache<V>(options?.cache);
    this.leases = new MemoryLeases(options?.leases);
  }
}

export class RedisAdapter<V> implements ProviderAdapter<V> {
  cache: RedisCache<V>;
  leases: RedisLeases;

  constructor(options: RedisAdapterOptions<V>) {
    this.cache = new RedisCache<V>({ client: options.client, ...(options.cache ?? {}) });
    this.leases = new RedisLeases({ client: options.client, ...(options.leases ?? {}) });
  }
}

export class MemcachedAdapter<V> implements ProviderAdapter<V> {
  cache: MemcachedCache<V>;

  constructor(options: MemcachedAdapterOptions<V>) {
    this.cache = new MemcachedCache<V>(options);
  }
}

export class DynamoDbAdapter<V> implements ProviderAdapter<V> {
  cache: DynamoDbCache<V>;

  constructor(options: DynamoDbAdapterOptions<V>) {
    this.cache = new DynamoDbCache<V>(options);
  }
}

export class MongoAdapter<V> implements ProviderAdapter<V> {
  cache: MongoCache<V>;
  leases: MongoLeases;

  constructor(options: MongoAdapterOptions<V>) {
    this.cache = new MongoCache<V>(options.cache);
    this.leases = new MongoLeases(options.leases);
  }
}

export class PostgresAdapter<V> implements ProviderAdapter<V> {
  cache: PostgresCache<V>;
  leases: PostgresLeases;

  constructor(options: PostgresAdapterOptions<V>) {
    this.cache = new PostgresCache<V>(options.cache);
    this.leases = new PostgresLeases(options.leases);
  }
}

export class S3Adapter<V> implements ProviderAdapter<V> {
  cache: S3Cache<V>;
  leases: S3Leases;

  constructor(options: S3AdapterOptions<V>) {
    this.cache = new S3Cache<V>(options.cache);
    this.leases = new S3Leases(options.leases);
  }
}

export class R2Adapter<V> implements ProviderAdapter<V> {
  cache: R2Cache<V>;
  leases: R2Leases;

  constructor(options: R2AdapterOptions<V>) {
    this.cache = new R2Cache<V>(options.cache);
    this.leases = new R2Leases(options.leases);
  }
}

export class CloudflareKvAdapter<V> implements ProviderAdapter<V> {
  cache: CloudflareKvCache<V>;
  leases: CloudflareD1Leases;

  constructor(options: CloudflareKvAdapterOptions<V>) {
    this.cache = new CloudflareKvCache<V>({ kv: options.kv, ...(options.cache ?? {}) });
    this.leases = new CloudflareD1Leases({ db: options.leasesDb, ...(options.leases ?? {}) });
  }
}

export class CloudflareD1Adapter<V> implements ProviderAdapter<V> {
  cache: CloudflareD1Cache<V>;
  leases: CloudflareD1Leases;

  constructor(options: CloudflareD1AdapterOptions<V>) {
    this.cache = new CloudflareD1Cache<V>({ db: options.db, ...(options.cache ?? {}) });
    this.leases = new CloudflareD1Leases({ db: options.db, ...(options.leases ?? {}) });
  }
}

export type AdapterType =
  | 'memory'
  | 'redis'
  | 'memcached'
  | 'dynamodb'
  | 'mongodb'
  | 'postgres'
  | 'cloudflare-kv'
  | 'cloudflare-d1'
  | 's3'
  | 'r2';

export type AdapterConfig<V> =
  | { type: 'memory'; options?: MemoryAdapterOptions }
  | { type: 'redis'; options: RedisAdapterOptions<V> }
  | { type: 'memcached'; options: MemcachedAdapterOptions<V> }
  | { type: 'dynamodb'; options: DynamoDbAdapterOptions<V> }
  | { type: 'mongodb'; options: MongoAdapterOptions<V> }
  | { type: 'postgres'; options: PostgresAdapterOptions<V> }
  | { type: 'cloudflare-kv'; options: CloudflareKvAdapterOptions<V> }
  | { type: 'cloudflare-d1'; options: CloudflareD1AdapterOptions<V> }
  | { type: 's3'; options: S3AdapterOptions<V> }
  | { type: 'r2'; options: R2AdapterOptions<V> };

export const createAdapter = <V>(config: AdapterConfig<V>): ProviderAdapter<V> => {
  switch (config.type) {
    case 'memory':
      return new MemoryAdapter<V>(config.options);
    case 'redis':
      return new RedisAdapter<V>(config.options);
    case 'memcached':
      return new MemcachedAdapter<V>(config.options);
    case 'dynamodb':
      return new DynamoDbAdapter<V>(config.options);
    case 'mongodb':
      return new MongoAdapter<V>(config.options);
    case 'postgres':
      return new PostgresAdapter<V>(config.options);
    case 'cloudflare-kv':
      return new CloudflareKvAdapter<V>(config.options);
    case 'cloudflare-d1':
      return new CloudflareD1Adapter<V>(config.options);
    case 's3':
      return new S3Adapter<V>(config.options);
    case 'r2':
      return new R2Adapter<V>(config.options);
    default: {
      const _exhaustive: never = config;
      throw new Error(`Unknown adapter: ${String((_exhaustive as { type?: unknown }).type ?? config)}`);
    }
  }
};

export const createMemoryAdapter = <V>(options?: MemoryAdapterOptions): MemoryAdapter<V> =>
  new MemoryAdapter<V>(options);
export const createRedisAdapter = <V>(options: RedisAdapterOptions<V>): RedisAdapter<V> => new RedisAdapter<V>(options);
export const createMemcachedAdapter = <V>(options: MemcachedAdapterOptions<V>): MemcachedAdapter<V> =>
  new MemcachedAdapter<V>(options);
export const createDynamoDbAdapter = <V>(options: DynamoDbAdapterOptions<V>): DynamoDbAdapter<V> =>
  new DynamoDbAdapter<V>(options);
export const createMongoAdapter = <V>(options: MongoAdapterOptions<V>): MongoAdapter<V> => new MongoAdapter<V>(options);
export const createPostgresAdapter = <V>(options: PostgresAdapterOptions<V>): PostgresAdapter<V> =>
  new PostgresAdapter<V>(options);
export const createS3Adapter = <V>(options: S3AdapterOptions<V>): S3Adapter<V> => new S3Adapter<V>(options);
export const createR2Adapter = <V>(options: R2AdapterOptions<V>): R2Adapter<V> => new R2Adapter<V>(options);
export const createCloudflareKvAdapter = <V>(options: CloudflareKvAdapterOptions<V>): CloudflareKvAdapter<V> =>
  new CloudflareKvAdapter<V>(options);
export const createCloudflareD1Adapter = <V>(options: CloudflareD1AdapterOptions<V>): CloudflareD1Adapter<V> =>
  new CloudflareD1Adapter<V>(options);
