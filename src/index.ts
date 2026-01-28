import { getOrSet } from '@core/factory';

/** Cache namespace for cache locking operations. */
export const Cache = {
  getOrSet,
};

/** Cache outcome enum values. */
export { CacheOutcome } from '@core/types';
/** Core types for cache locking. */
export type {
  AdapterInput,
  CacheLockingErrorCode,
  CacheLockingErrorContext,
  CacheLockingHooks,
  CacheLockingHookResult,
  CacheMeta,
  CoreClock,
  Fetcher,
  FetcherContext,
  GetOrSetOptions,
  GetOrSetOnceOptions,
  GetOrSetResult,
  HookEffect,
  LeaseAcquireResult,
  LeaseReadyState,
  Leases,
  OwnerId,
  Sleep,
  WaitStrategy,
  WaitStrategyContext,
} from '@core/types';

/** Adapter configuration types. */
export type {
  AdapterConfig,
  AdapterType,
  CloudflareD1AdapterOptions,
  CloudflareKvAdapterOptions,
  DynamoDbAdapterOptions,
  MemcachedAdapterOptions,
  MemoryAdapterOptions,
  MongoAdapterOptions,
  PostgresAdapterOptions,
  ProviderAdapter,
  R2AdapterOptions,
  RedisAdapterOptions,
  S3AdapterOptions,
} from '@adapters/factory';

/** Cache locking error types and utilities. */
export {
  AdapterError,
  AbortedError,
  CacheGetFailed,
  CacheSetFailed,
  FetcherFailed,
  HookFailed,
  formatCacheLockingError,
  isAdapterError,
  isCacheLockingError,
  LeaseAcquireFailed,
  LeaseReadyFailed,
  LeaseReleaseFailed,
  matchCacheLockingError,
  ValidationError,
  WaitFailed,
  WaitStrategyFailed,
} from '@core/errors';
/** Error matcher and type exports. */
export type { AdapterOperation, CacheLockingError, CacheLockingErrorMatcher } from '@core/errors';
