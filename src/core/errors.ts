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

const CacheLockingErrorContextSchema = Schema.Struct({
  key: Schema.optional(Schema.String),
  phase: PhaseSchema,
  adapter: Schema.optional(PhaseAdapterSchema),
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

export class ValidationError extends Schema.TaggedError<ValidationError>()(
  'VALIDATION_ERROR',
  ValidationErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, issues?: ReadonlyArray<string>, cause?: unknown) {
    super({ message, context, issues, cause });
  }
}

export class CacheGetFailed extends Schema.TaggedError<CacheGetFailed>()(
  'CACHE_GET_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class CacheSetFailed extends Schema.TaggedError<CacheSetFailed>()(
  'CACHE_SET_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class LeaseAcquireFailed extends Schema.TaggedError<LeaseAcquireFailed>()(
  'LEASE_ACQUIRE_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class LeaseReleaseFailed extends Schema.TaggedError<LeaseReleaseFailed>()(
  'LEASE_RELEASE_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class LeaseReadyFailed extends Schema.TaggedError<LeaseReadyFailed>()(
  'LEASE_READY_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class FetcherFailed extends Schema.TaggedError<FetcherFailed>()(
  'FETCHER_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class HookFailed extends Schema.TaggedError<HookFailed>()('HOOK_FAILED', CacheLockingErrorFieldsSchema) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class WaitStrategyFailed extends Schema.TaggedError<WaitStrategyFailed>()(
  'WAIT_STRATEGY_FAILED',
  CacheLockingErrorFieldsSchema,
) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class WaitFailed extends Schema.TaggedError<WaitFailed>()('WAIT_FAILED', CacheLockingErrorFieldsSchema) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

export class AbortedError extends Schema.TaggedError<AbortedError>()('ABORTED', CacheLockingErrorFieldsSchema) {
  constructor(message: string, context: CacheLockingErrorContext, cause?: unknown) {
    super({ message, context, cause });
  }
}

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

export const isCacheLockingError = (error: unknown): error is CacheLockingError => {
  if (!error || typeof error !== 'object') {
    return false;
  }
  const tag = (error as { _tag?: string })._tag;
  return typeof tag === 'string' && errorTags.has(tag as CacheLockingErrorCode);
};
