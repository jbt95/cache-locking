import { Effect } from 'effect';
import type { CacheLockingError } from '@core/errors';
import {
  AbortedError,
  CacheGetFailed,
  CacheSetFailed,
  FetcherFailed,
  HookFailed,
  isCacheLockingError,
  LeaseAcquireFailed,
  LeaseReadyFailed,
  LeaseReleaseFailed,
  ValidationError,
  WaitFailed,
  WaitStrategyFailed,
} from '@core/errors';
import { Phase } from '@core/phases';
import type { CacheLockingErrorContext, PhaseAdapter } from '@core/types';

type PhaseConfig = {
  adapter?: PhaseAdapter;
  toError: (message: string, context: CacheLockingErrorContext, cause?: unknown) => CacheLockingError;
};

const phaseConfig: Record<Phase, PhaseConfig> = {
  [Phase.Validation]: {
    adapter: 'validation',
    toError: (message, context, cause) => new ValidationError(message, context, undefined, cause),
  },
  [Phase.CacheGet]: {
    adapter: 'cache',
    toError: (message, context, cause) => new CacheGetFailed(message, context, cause),
  },
  [Phase.CacheSet]: {
    adapter: 'cache',
    toError: (message, context, cause) => new CacheSetFailed(message, context, cause),
  },
  [Phase.LeaseAcquire]: {
    adapter: 'leases',
    toError: (message, context, cause) => new LeaseAcquireFailed(message, context, cause),
  },
  [Phase.LeaseRelease]: {
    adapter: 'leases',
    toError: (message, context, cause) => new LeaseReleaseFailed(message, context, cause),
  },
  [Phase.LeaseMarkReady]: {
    adapter: 'leases',
    toError: (message, context, cause) => new LeaseReadyFailed(message, context, cause),
  },
  [Phase.LeaseIsReady]: {
    adapter: 'leases',
    toError: (message, context, cause) => new LeaseReadyFailed(message, context, cause),
  },
  [Phase.Fetcher]: {
    adapter: 'fetcher',
    toError: (message, context, cause) => new FetcherFailed(message, context, cause),
  },
  [Phase.HooksOnHit]: {
    adapter: 'hooks',
    toError: (message, context, cause) => new HookFailed(message, context, cause),
  },
  [Phase.HooksOnLeader]: {
    adapter: 'hooks',
    toError: (message, context, cause) => new HookFailed(message, context, cause),
  },
  [Phase.HooksOnFollowerWait]: {
    adapter: 'hooks',
    toError: (message, context, cause) => new HookFailed(message, context, cause),
  },
  [Phase.HooksOnFallback]: {
    adapter: 'hooks',
    toError: (message, context, cause) => new HookFailed(message, context, cause),
  },
  [Phase.WaitStrategy]: {
    adapter: 'wait',
    toError: (message, context, cause) => new WaitStrategyFailed(message, context, cause),
  },
  [Phase.WaitSleep]: {
    adapter: 'wait',
    toError: (message, context, cause) => new WaitFailed(message, context, cause),
  },
  [Phase.Abort]: {
    adapter: 'wait',
    toError: (message, context, cause) => new AbortedError(message, context, cause),
  },
};

type PhaseContext = Omit<CacheLockingErrorContext, 'phase' | 'adapter'>;

const cloneCacheLockingError = (error: CacheLockingError): CacheLockingError => {
  switch (error._tag) {
    case 'VALIDATION_ERROR':
      return new ValidationError(error.message, error.context, error.issues, error.cause);
    case 'CACHE_GET_FAILED':
      return new CacheGetFailed(error.message, error.context, error.cause);
    case 'CACHE_SET_FAILED':
      return new CacheSetFailed(error.message, error.context, error.cause);
    case 'LEASE_ACQUIRE_FAILED':
      return new LeaseAcquireFailed(error.message, error.context, error.cause);
    case 'LEASE_RELEASE_FAILED':
      return new LeaseReleaseFailed(error.message, error.context, error.cause);
    case 'LEASE_READY_FAILED':
      return new LeaseReadyFailed(error.message, error.context, error.cause);
    case 'FETCHER_FAILED':
      return new FetcherFailed(error.message, error.context, error.cause);
    case 'HOOK_FAILED':
      return new HookFailed(error.message, error.context, error.cause);
    case 'WAIT_STRATEGY_FAILED':
      return new WaitStrategyFailed(error.message, error.context, error.cause);
    case 'WAIT_FAILED':
      return new WaitFailed(error.message, error.context, error.cause);
    case 'ABORTED':
      return new AbortedError(error.message, error.context, error.cause);
    default: {
      const _exhaustive: never = error;
      return _exhaustive;
    }
  }
};

export class PhaseRunner {
  private buildContext(phase: Phase, context: PhaseContext): CacheLockingErrorContext {
    const config = phaseConfig[phase];
    return Object.assign({}, context, {
      phase,
      adapter: config.adapter,
    });
  }

  private mapError(
    phase: Phase,
    context: CacheLockingErrorContext,
    message: string,
    cause: unknown,
  ): CacheLockingError {
    if (isCacheLockingError(cause)) {
      return cloneCacheLockingError(cause);
    }
    const config = phaseConfig[phase];
    return config.toError(message, context, cause);
  }

  private runEffect<T>(
    phase: Phase,
    context: CacheLockingErrorContext,
    effect: Effect.Effect<T, CacheLockingError>,
  ): Effect.Effect<T, CacheLockingError> {
    return effect.pipe(
      Effect.annotateLogs({
        phase,
        key: context.key ?? '',
        adapter: context.adapter ?? '',
      }),
      Effect.withSpan(`cache-locking.${phase}`, {
        attributes: {
          key: context.key ?? '',
          adapter: context.adapter ?? '',
        },
      }),
    );
  }

  runPromise<T>(
    phase: Phase,
    context: PhaseContext,
    message: string,
    action: () => Promise<T>,
  ): Effect.Effect<T, CacheLockingError> {
    const fullContext = this.buildContext(phase, context);
    const effect = Effect.tryPromise({
      try: () => action(),
      catch: (cause) => this.mapError(phase, fullContext, message, cause),
    });
    return this.runEffect(phase, fullContext, effect);
  }

  runSync<T>(
    phase: Phase,
    context: PhaseContext,
    message: string,
    action: () => T,
  ): Effect.Effect<T, CacheLockingError> {
    const fullContext = this.buildContext(phase, context);
    const effect = Effect.try({
      try: () => action(),
      catch: (cause) => this.mapError(phase, fullContext, message, cause),
    });
    return this.runEffect(phase, fullContext, effect);
  }
}
