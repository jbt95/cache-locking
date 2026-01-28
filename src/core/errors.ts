import { Schema } from 'effect';
import type { CacheLockingErrorContext } from '@core/types';
import { Phase } from '@core/phases';

const PhaseSchema = Schema.Union(
  Schema.Literal(Phase.Validation),
  Schema.Literal(Phase.CacheGet),
  Schema.Literal(Phase.CacheSet),
  Schema.Literal(Phase.LeaseAcquire),
  Schema.Literal(Phase.LeaseRelease),
  Schema.Literal(Phase.LeaseMarkReady),
  Schema.Literal(Phase.LeaseIsReady),
  Schema.Literal(Phase.Fetcher),
  Schema.Literal(Phase.HooksOnHit),
  Schema.Literal(Phase.HooksOnLeader),
  Schema.Literal(Phase.HooksOnFollowerWait),
  Schema.Literal(Phase.HooksOnFallback),
  Schema.Literal(Phase.WaitStrategy),
  Schema.Literal(Phase.WaitSleep),
  Schema.Literal(Phase.Abort),
);

const PhaseAdapterSchema = Schema.Union(
  Schema.Literal('cache'),
  Schema.Literal('leases'),
  Schema.Literal('fetcher'),
  Schema.Literal('hooks'),
  Schema.Literal('wait'),
  Schema.Literal('validation'),
);

/** Adapter operation identifier for error mapping. */
export type AdapterOperation =
  | 'cache.get'
  | 'cache.set'
  | 'leases.acquire'
  | 'leases.release'
  | 'leases.markReady'
  | 'leases.isReady';

const AdapterOperationSchema = Schema.Union(
  Schema.Literal('cache.get'),
  Schema.Literal('cache.set'),
  Schema.Literal('leases.acquire'),
  Schema.Literal('leases.release'),
  Schema.Literal('leases.markReady'),
  Schema.Literal('leases.isReady'),
);

const CacheLockingErrorContextSchema = Schema.Struct({
  key: Schema.optional(Schema.String),
  phase: PhaseSchema,
  adapter: Schema.optional(PhaseAdapterSchema),
});

const AdapterErrorFieldsSchema = Schema.Struct({
  message: Schema.String,
  operation: AdapterOperationSchema,
  key: Schema.String,
  cause: Schema.optional(Schema.Unknown),
});

const CacheLockingErrorFieldsSchema = Schema.Struct({
  message: Schema.String,
  context: CacheLockingErrorContextSchema,
  cause: Schema.optional(Schema.Unknown),
});

const ValidationErrorFieldsSchema = Schema.Struct({
  message: Schema.String,
  context: CacheLockingErrorContextSchema,
  issues: Schema.optional(Schema.Array(Schema.String)),
  cause: Schema.optional(Schema.Unknown),
});

/** Validation errors for input or configuration. */
export class ValidationError extends Schema.TaggedError<ValidationError>()(
  'VALIDATION_ERROR',
  ValidationErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, issues?: ReadonlyArray<string>, cause?: unknown) {
    super({ message, context, issues, cause });
  }
}

/** Adapter error raised by cache or lease backends. */
export class AdapterError extends Schema.TaggedError<AdapterError>()('ADAPTER_ERROR', AdapterErrorFieldsSchema) {
  constructor(operation: AdapterOperation, key: string, cause?: unknown) {
    super({
      message: `${operation} failed for key "${key}"`,
      operation,
      key,
      cause,
    });
  }
}

/** Cache get failure mapped from adapter or runtime. */
export class CacheGetFailed extends Schema.TaggedError<CacheGetFailed>()(
  'CACHE_GET_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Cache set failure mapped from adapter or runtime. */
export class CacheSetFailed extends Schema.TaggedError<CacheSetFailed>()(
  'CACHE_SET_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Lease acquire failure mapped from adapter or runtime. */
export class LeaseAcquireFailed extends Schema.TaggedError<LeaseAcquireFailed>()(
  'LEASE_ACQUIRE_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Lease release failure mapped from adapter or runtime. */
export class LeaseReleaseFailed extends Schema.TaggedError<LeaseReleaseFailed>()(
  'LEASE_RELEASE_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Lease readiness failure mapped from adapter or runtime. */
export class LeaseReadyFailed extends Schema.TaggedError<LeaseReadyFailed>()(
  'LEASE_READY_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Fetcher failure mapped into cache locking errors. */
export class FetcherFailed extends Schema.TaggedError<FetcherFailed>()(
  'FETCHER_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Hook failure mapped into cache locking errors. */
export class HookFailed extends Schema.TaggedError<HookFailed>()('HOOK_FAILED', CacheLockingErrorFieldsSchema) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Wait strategy failure mapped into cache locking errors. */
export class WaitStrategyFailed extends Schema.TaggedError<WaitStrategyFailed>()(
  'WAIT_STRATEGY_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Wait sleep failure mapped into cache locking errors. */
export class WaitFailed extends Schema.TaggedError<WaitFailed>()('WAIT_FAILED', CacheLockingErrorFieldsSchema) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Abort error when AbortSignal cancels a request. */
export class AbortedError extends Schema.TaggedError<AbortedError>()('ABORTED', CacheLockingErrorFieldsSchema) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

/** Union of all cache locking error types. */
export type CacheLockingError =
  | ValidationError
  | CacheGetFailed
  | CacheSetFailed
  | LeaseAcquireFailed
  | LeaseReleaseFailed
  | LeaseReadyFailed
  | FetcherFailed
  | HookFailed
  | WaitStrategyFailed
  | WaitFailed
  | AbortedError;

type CacheLockingErrorCode = CacheLockingError['_tag'];

const errorTags = new Set<CacheLockingErrorCode>([
  'VALIDATION_ERROR',
  'CACHE_GET_FAILED',
  'CACHE_SET_FAILED',
  'LEASE_ACQUIRE_FAILED',
  'LEASE_RELEASE_FAILED',
  'LEASE_READY_FAILED',
  'FETCHER_FAILED',
  'HOOK_FAILED',
  'WAIT_STRATEGY_FAILED',
  'WAIT_FAILED',
  'ABORTED',
]);

/** Type guard for cache locking errors. */
export const isCacheLockingError = (error: unknown): error is CacheLockingError => {
  if (!error || typeof error !== 'object') {
    return false;
  }
  const tag = (error as { _tag?: string })._tag;
  return typeof tag === 'string' && errorTags.has(tag as CacheLockingErrorCode);
};

/** Type guard for adapter errors. */
export const isAdapterError = (error: unknown): error is AdapterError => {
  if (!error || typeof error !== 'object') {
    return false;
  }
  return (error as { _tag?: string })._tag === 'ADAPTER_ERROR';
};

/** Matcher for cache locking errors with a default case. */
export type CacheLockingErrorMatcher<A> = {
  VALIDATION_ERROR?: (error: ValidationError) => A;
  CACHE_GET_FAILED?: (error: CacheGetFailed) => A;
  CACHE_SET_FAILED?: (error: CacheSetFailed) => A;
  LEASE_ACQUIRE_FAILED?: (error: LeaseAcquireFailed) => A;
  LEASE_RELEASE_FAILED?: (error: LeaseReleaseFailed) => A;
  LEASE_READY_FAILED?: (error: LeaseReadyFailed) => A;
  FETCHER_FAILED?: (error: FetcherFailed) => A;
  HOOK_FAILED?: (error: HookFailed) => A;
  WAIT_STRATEGY_FAILED?: (error: WaitStrategyFailed) => A;
  WAIT_FAILED?: (error: WaitFailed) => A;
  ABORTED?: (error: AbortedError) => A;
  _: (error: CacheLockingError) => A;
};

/** Pattern match on cache locking errors by tag. */
export const matchCacheLockingError = <A>(error: CacheLockingError, matcher: CacheLockingErrorMatcher<A>): A => {
  switch (error._tag) {
    case 'VALIDATION_ERROR':
      return matcher.VALIDATION_ERROR ? matcher.VALIDATION_ERROR(error) : matcher._(error);
    case 'CACHE_GET_FAILED':
      return matcher.CACHE_GET_FAILED ? matcher.CACHE_GET_FAILED(error) : matcher._(error);
    case 'CACHE_SET_FAILED':
      return matcher.CACHE_SET_FAILED ? matcher.CACHE_SET_FAILED(error) : matcher._(error);
    case 'LEASE_ACQUIRE_FAILED':
      return matcher.LEASE_ACQUIRE_FAILED ? matcher.LEASE_ACQUIRE_FAILED(error) : matcher._(error);
    case 'LEASE_RELEASE_FAILED':
      return matcher.LEASE_RELEASE_FAILED ? matcher.LEASE_RELEASE_FAILED(error) : matcher._(error);
    case 'LEASE_READY_FAILED':
      return matcher.LEASE_READY_FAILED ? matcher.LEASE_READY_FAILED(error) : matcher._(error);
    case 'FETCHER_FAILED':
      return matcher.FETCHER_FAILED ? matcher.FETCHER_FAILED(error) : matcher._(error);
    case 'HOOK_FAILED':
      return matcher.HOOK_FAILED ? matcher.HOOK_FAILED(error) : matcher._(error);
    case 'WAIT_STRATEGY_FAILED':
      return matcher.WAIT_STRATEGY_FAILED ? matcher.WAIT_STRATEGY_FAILED(error) : matcher._(error);
    case 'WAIT_FAILED':
      return matcher.WAIT_FAILED ? matcher.WAIT_FAILED(error) : matcher._(error);
    case 'ABORTED':
      return matcher.ABORTED ? matcher.ABORTED(error) : matcher._(error);
    default: {
      const _exhaustive: never = error;
      return matcher._(error ?? _exhaustive);
    }
  }
};

/** Format a cache locking error into a concise string. */
export const formatCacheLockingError = (error: CacheLockingError): string => {
  const parts = [error._tag, error.message];
  if (error.context.key) {
    parts.push(`key=${error.context.key}`);
  }
  parts.push(`phase=${error.context.phase}`);
  if (error.context.adapter) {
    parts.push(`adapter=${error.context.adapter}`);
  }
  if (error.cause instanceof Error) {
    parts.push(`cause=${error.cause.message}`);
  } else if (error.cause !== undefined) {
    parts.push(`cause=${String(error.cause)}`);
  }
  return parts.join(' | ');
};
