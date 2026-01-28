import type { Cache, Leases } from '@core/types';
import { S3Cache, S3Leases, type S3CacheOptions, type S3LeasesOptions } from '@adapters/s3';

/** R2 cache options (S3-compatible). */
export type R2CacheOptions<V> = S3CacheOptions<V>;
/** R2 leases options (S3-compatible). */
export type R2LeasesOptions = S3LeasesOptions;

/** R2 cache adapter. */
export class R2Cache<V> extends S3Cache<V> {}
/** R2 leases adapter. */
export class R2Leases extends S3Leases {}

/** Create an R2 cache adapter instance. */
export const createR2Cache = <V>(options: R2CacheOptions<V>): Cache<V> => new R2Cache(options);
/** Create an R2 leases adapter instance. */
export const createR2Leases = (options: R2LeasesOptions): Leases => new R2Leases(options);
