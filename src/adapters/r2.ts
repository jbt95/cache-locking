import type { Cache, Leases } from '@core/types';
import { S3Cache, S3Leases, type S3CacheOptions, type S3LeasesOptions } from '@adapters/s3';

export type R2CacheOptions<V> = S3CacheOptions<V>;
export type R2LeasesOptions = S3LeasesOptions;

export class R2Cache<V> extends S3Cache<V> {}
export class R2Leases extends S3Leases {}

export const createR2Cache = <V>(options: R2CacheOptions<V>): Cache<V> => new R2Cache(options);
export const createR2Leases = (options: R2LeasesOptions): Leases => new R2Leases(options);
