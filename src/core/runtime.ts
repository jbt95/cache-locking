import { Clock, Duration, Effect, Option, Ref, Schedule, Schema } from 'effect';
import type {
  Cache,
  Fetcher,
  FetcherContext,
  GetOrSetOptions,
  GetOrSetResult,
  Key,
  LeaseAcquireResult,
  LeaseReadyState,
  Leases,
  OwnerId,
  ResolvedDefaults,
  ResolvedOptions,
  ValidatedGetOrSetOptions,
  WaitOutcome,
  WaitStrategy,
  WaitStrategyContext,
} from '@core/types';
import { CacheOutcome } from '@core/types';
import { resolveOptionalDuration } from '@core/defaults';
import { callOptionsSchema, decodeKey, decodeWith, fetcherSchema, waitDelaySchema } from '@core/validation';
import type { CacheLockingError } from '@core/errors';
import { AbortedError } from '@core/errors';
import { HookRunner } from '@core/hooks';
import { Phase } from '@core/phases';
import { PhaseRunner } from '@core/phase-runner';
import type { CacheLockingConfig } from '@core/services';
import { CacheLockingConfigService, type CacheLockingEnv, CacheService, LeasesService } from '@core/services';

class CallContext<V> {
  constructor(
    public readonly key: Key,
    public readonly options: ResolvedOptions<V>,
    public readonly hooks: HookRunner<V>,
  ) {}
}

const WaitRetrySchema = Schema.Struct({
  delay: Schema.DurationFromSelf,
});

class WaitRetry extends Schema.TaggedError<WaitRetry>()('WAIT_RETRY', WaitRetrySchema) {
  constructor(delay: Duration.Duration) {
    super({ delay });
  }
}

const WaitTimeoutSchema = Schema.Struct({
  key: Schema.String,
});

class WaitTimeout extends Schema.TaggedError<WaitTimeout>()('WAIT_TIMEOUT', WaitTimeoutSchema) {
  constructor(key: Key) {
    super({ key });
  }
}

const isWaitRetry = (error: unknown): error is WaitRetry =>
  !!error && typeof error === 'object' && (error as { _tag?: string })._tag === 'WAIT_RETRY';

export class CacheLockingRuntime<V> {
  private readonly phaseRunner = new PhaseRunner();

  getOrSetEffect = (
    key: string,
    fetcher: Fetcher<V>,
    opts?: GetOrSetOptions<V>,
  ): Effect.Effect<GetOrSetResult<V>, CacheLockingError, CacheLockingEnv> => {
    const runtime = this;

    return Effect.gen(function* () {
      const config = yield* runtime.configEffect();
      const validatedKey = yield* runtime.validateGetOrSetArgsEffect(key, fetcher, config.validateOptions);
      const call = yield* runtime.createCallContextEffect(validatedKey, opts, config);

      const flow = Effect.gen(function* () {
        const cached = yield* runtime.cacheGet(call.key);
        if (Option.isSome(cached)) {
          yield* runtime.hookOnHit(call, cached.value);
          return {
            value: cached.value,
            meta: CacheLockingRuntime.buildMeta(CacheOutcome.HIT),
          } satisfies GetOrSetResult<V>;
        }

        const lease = yield* runtime.leaseAcquire(call.key, call.options.ownerId, call.options.leaseTtl);
        if (lease.role === 'leader') {
          return yield* runtime.runLeaderWithRelease(call, lease, fetcher);
        }

        return yield* runtime.runFollower(call, lease, fetcher);
      });

      return yield* runtime.withAbortSignal(call.key, call.options.signal, flow);
    });
  };

  private static buildMeta<V>(
    cache: CacheOutcome,
    leaseUntil?: number,
    waited?: Duration.Duration,
  ): GetOrSetResult<V>['meta'] {
    return {
      cache,
      leaseUntil,
      waited,
    };
  }

  private configEffect(): Effect.Effect<CacheLockingConfig<V>, never, CacheLockingEnv> {
    return CacheLockingConfigService.pipe(Effect.map((config) => config as CacheLockingConfig<V>));
  }

  private cacheService(): Effect.Effect<Cache<V>, never, CacheLockingEnv> {
    return CacheService.pipe(Effect.map((cache) => cache as Cache<V>));
  }

  private leasesService(): Effect.Effect<Leases, never, CacheLockingEnv> {
    return LeasesService;
  }

  private validateGetOrSetArgsEffect(
    key: string,
    fetcher: Fetcher<V>,
    validateOptions: boolean,
  ): Effect.Effect<Key, CacheLockingError, CacheLockingEnv> {
    if (!validateOptions) {
      return Effect.succeed(key as Key);
    }

    const context = { key, phase: Phase.Validation, adapter: 'validation' } as const;

    return Effect.gen(function* () {
      const validatedKey = yield* decodeKey(key, context);
      yield* decodeWith(fetcherSchema, fetcher, 'fetcher', context);
      return validatedKey;
    });
  }

  private parseCallOptionsEffect(
    key: Key,
    overrides: GetOrSetOptions<V> | undefined,
    validateOptions: boolean,
  ): Effect.Effect<ValidatedGetOrSetOptions<V>, CacheLockingError, CacheLockingEnv> {
    if (!validateOptions) {
      return Effect.succeed({
        cacheTtl: resolveOptionalDuration(overrides?.cacheTtl, undefined),
        leaseTtl: resolveOptionalDuration(overrides?.leaseTtl, undefined),
        waitMax: resolveOptionalDuration(overrides?.waitMax, undefined),
        waitStep: resolveOptionalDuration(overrides?.waitStep, undefined),
        shouldCache: overrides?.shouldCache,
        ownerId: overrides?.ownerId as ValidatedGetOrSetOptions<V>['ownerId'],
        signal: overrides?.signal,
        waitStrategy: overrides?.waitStrategy,
        hooks: overrides?.hooks,
      });
    }

    return decodeWith(callOptionsSchema, overrides ?? {}, 'getOrSet options', {
      key,
      phase: Phase.Validation,
      adapter: 'validation',
    }).pipe(Effect.map((value) => value as ValidatedGetOrSetOptions<V>));
  }

  private resolveCallOptions(
    defaults: ResolvedDefaults<V>,
    parsedOverrides: ValidatedGetOrSetOptions<V>,
  ): ResolvedOptions<V> {
    return {
      leaseTtl: parsedOverrides.leaseTtl ?? defaults.leaseTtl,
      waitMax: parsedOverrides.waitMax ?? defaults.waitMax,
      waitStep: parsedOverrides.waitStep ?? defaults.waitStep,
      shouldCache: parsedOverrides.shouldCache ?? defaults.shouldCache,
      cacheTtl: parsedOverrides.cacheTtl ?? defaults.cacheTtl,
      ownerId: parsedOverrides.ownerId ?? defaults.ownerId,
      signal: parsedOverrides.signal ?? defaults.signal,
      waitStrategy: parsedOverrides.waitStrategy ?? defaults.waitStrategy,
      hooks: parsedOverrides.hooks ?? defaults.hooks,
    };
  }

  private createCallContextEffect(
    key: Key,
    overrides: GetOrSetOptions<V> | undefined,
    config: CacheLockingConfig<V>,
  ): Effect.Effect<CallContext<V>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;

    return Effect.gen(function* () {
      const parsedOverrides = yield* runtime.parseCallOptionsEffect(key, overrides, config.validateOptions);
      const resolved = runtime.resolveCallOptions(config.defaults, parsedOverrides);
      const hooks = new HookRunner(config.defaults.hooks, parsedOverrides.hooks);
      return new CallContext(key, resolved, hooks);
    });
  }

  private createAbortedError(key: Key, cause?: unknown): AbortedError {
    return new AbortedError(`operation aborted for key "${key}"`, { key, phase: Phase.Abort, adapter: 'wait' }, cause);
  }

  private abortEffect(key: Key, signal: AbortSignal): Effect.Effect<never, AbortedError> {
    const runtime = this;
    return Effect.async<never, AbortedError>((resume) => {
      if (signal.aborted) {
        resume(Effect.fail(runtime.createAbortedError(key, signal.reason)));
        return;
      }
      const onAbort = () => {
        resume(Effect.fail(runtime.createAbortedError(key, signal.reason)));
      };
      signal.addEventListener('abort', onAbort, { once: true });
      return Effect.sync(() => {
        signal.removeEventListener('abort', onAbort);
      });
    });
  }

  private withAbortSignal<A>(
    key: Key,
    signal: AbortSignal | undefined,
    effect: Effect.Effect<A, CacheLockingError, CacheLockingEnv>,
  ): Effect.Effect<A, CacheLockingError, CacheLockingEnv> {
    if (!signal) {
      return effect;
    }
    if (signal.aborted) {
      return Effect.fail(this.createAbortedError(key, signal.reason));
    }
    return Effect.race(effect, this.abortEffect(key, signal));
  }

  private cacheGet(key: Key): Effect.Effect<Option.Option<V>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const cache = yield* runtime.cacheService();
      const value = yield* runtime.phaseRunner.runPromise(
        Phase.CacheGet,
        { key },
        `cache.get failed for key "${key}"`,
        () => cache.get(key),
      );
      return Option.fromNullable(value);
    });
  }

  private cacheSet(
    key: Key,
    value: V,
    ttl?: Duration.DurationInput,
  ): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const cache = yield* runtime.cacheService();
      return yield* runtime.phaseRunner.runPromise(Phase.CacheSet, { key }, `cache.set failed for key "${key}"`, () =>
        cache.set(key, value, ttl),
      );
    });
  }

  private leaseAcquire(
    key: Key,
    owner: OwnerId,
    ttl: Duration.DurationInput,
  ): Effect.Effect<LeaseAcquireResult, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const leases = yield* runtime.leasesService();
      return yield* runtime.phaseRunner.runPromise(
        Phase.LeaseAcquire,
        { key },
        `leases.acquire failed for key "${key}"`,
        () => leases.acquire(key, owner, ttl),
      );
    });
  }

  private leaseRelease(key: Key, owner: OwnerId): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const leases = yield* runtime.leasesService();
      return yield* runtime.phaseRunner.runPromise(
        Phase.LeaseRelease,
        { key },
        `leases.release failed for key "${key}"`,
        () => leases.release(key, owner),
      );
    });
  }

  private leaseMarkReady(key: Key): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const leases = yield* runtime.leasesService();
      const markReady = leases.markReady?.bind(leases);
      if (!markReady) {
        return undefined;
      }
      return yield* runtime.phaseRunner.runPromise(
        Phase.LeaseMarkReady,
        { key },
        `leases.markReady failed for key "${key}"`,
        () => markReady(key),
      );
    });
  }

  private leaseIsReady(key: Key): Effect.Effect<Option.Option<LeaseReadyState>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const leases = yield* runtime.leasesService();
      const isReady = leases.isReady?.bind(leases);
      if (!isReady) {
        return Option.none<LeaseReadyState>();
      }
      const state = yield* runtime.phaseRunner.runPromise(
        Phase.LeaseIsReady,
        { key },
        `leases.isReady failed for key "${key}"`,
        () => isReady(key),
      );
      return Option.fromNullable(state);
    });
  }

  private fetchValue(
    key: Key,
    fetcher: Fetcher<V>,
    context: FetcherContext,
  ): Effect.Effect<V, CacheLockingError, CacheLockingEnv> {
    return this.phaseRunner.runPromise(Phase.Fetcher, { key }, `fetcher failed for key "${key}"`, () =>
      fetcher(context),
    );
  }

  private hookOnHit(call: CallContext<V>, value: V): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    return this.phaseRunner.runPromise(
      Phase.HooksOnHit,
      { key: call.key },
      `hooks.onHit failed for key "${call.key}"`,
      () => call.hooks.onHit(value, { key: call.key }),
    );
  }

  private hookOnLeader(
    call: CallContext<V>,
    value: V,
    leaseUntil: number,
    cached: boolean,
  ): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    return this.phaseRunner.runPromise(
      Phase.HooksOnLeader,
      { key: call.key },
      `hooks.onLeader failed for key "${call.key}"`,
      () => call.hooks.onLeader(value, { key: call.key, leaseUntil, cached }),
    );
  }

  private hookOnFollowerWait(
    call: CallContext<V>,
    leaseUntil: number,
    waited: Duration.Duration,
    outcome: 'HIT' | 'FALLBACK',
  ): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    return this.phaseRunner.runPromise(
      Phase.HooksOnFollowerWait,
      { key: call.key },
      `hooks.onFollowerWait failed for key "${call.key}"`,
      () =>
        call.hooks.onFollowerWait({
          key: call.key,
          leaseUntil,
          waited,
          outcome,
        }),
    );
  }

  private hookOnFallback(
    call: CallContext<V>,
    value: V,
    leaseUntil: number,
    waited: Duration.Duration,
  ): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    return this.phaseRunner.runPromise(
      Phase.HooksOnFallback,
      { key: call.key },
      `hooks.onFallback failed for key "${call.key}"`,
      () => call.hooks.onFallback(value, { key: call.key, leaseUntil, waited }),
    );
  }

  private resolveWaitDelay(
    strategy: WaitStrategy,
    context: WaitStrategyContext,
    key: Key,
  ): Effect.Effect<Duration.Duration, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return runtime.phaseRunner
      .runSync(Phase.WaitStrategy, { key }, `waitStrategy failed for key "${key}"`, () => strategy(context))
      .pipe(
        Effect.flatMap((delay) =>
          decodeWith(waitDelaySchema, delay, 'waitStrategy delay', { key, phase: Phase.WaitStrategy, adapter: 'wait' }),
        ),
      );
  }

  private waitForCache(call: CallContext<V>): Effect.Effect<WaitOutcome<V>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;

    return Effect.gen(function* () {
      const start = yield* Clock.currentTimeMillis;
      const attempts = yield* Ref.make(0);

      const poll = Effect.gen(function* () {
        const cached = yield* runtime.cacheGet(call.key);
        if (Option.isSome(cached)) {
          return cached;
        }

        const readyState = yield* runtime.leaseIsReady(call.key);
        if (Option.isSome(readyState) && (readyState.value.ready || readyState.value.expired)) {
          return Option.none<V>();
        }

        const attempt = yield* Ref.getAndUpdate(attempts, (value) => value + 1);
        const now = yield* Clock.currentTimeMillis;
        const elapsedMs = Math.max(0, now - start);
        const remainingMs = Math.max(0, Duration.toMillis(call.options.waitMax) - elapsedMs);

        if (remainingMs <= 0) {
          return Option.none<V>();
        }

        const waitContext: WaitStrategyContext = {
          attempt,
          elapsed: Duration.millis(elapsedMs),
          remaining: Duration.millis(remainingMs),
          waitMax: call.options.waitMax,
          waitStep: call.options.waitStep,
        };

        const delay = yield* runtime.resolveWaitDelay(call.options.waitStrategy, waitContext, call.key);
        const boundedDelay = Duration.min(delay, Duration.millis(remainingMs));
        return yield* Effect.fail(new WaitRetry(boundedDelay));
      });

      const retrySchedule = Schedule.recurWhile((error: CacheLockingError | WaitRetry) => isWaitRetry(error)).pipe(
        Schedule.addDelayEffect((error) => Effect.succeed(isWaitRetry(error) ? error.delay : Duration.zero)),
      );

      const waitResult = yield* poll.pipe(
        Effect.retry(retrySchedule),
        Effect.catchTag('WAIT_RETRY', () => Effect.dieMessage('wait retry escaped retry schedule')),
        Effect.timeoutFail({ duration: call.options.waitMax, onTimeout: () => new WaitTimeout(call.key) }),
        Effect.catchTag('WAIT_TIMEOUT', () => Effect.succeed(Option.none<V>())),
      );

      const end = yield* Clock.currentTimeMillis;
      const waited = Duration.millis(Math.max(0, end - start));

      if (Option.isSome(waitResult)) {
        return {
          value: waitResult,
          waited,
          outcome: 'HIT',
        } satisfies WaitOutcome<V>;
      }

      const cachedAfterWait = yield* runtime.cacheGet(call.key);
      return {
        value: cachedAfterWait,
        waited,
        outcome: Option.isSome(cachedAfterWait) ? 'HIT' : 'FALLBACK',
      } satisfies WaitOutcome<V>;
    });
  }

  private runLeader(
    call: CallContext<V>,
    lease: LeaseAcquireResult,
    fetcher: Fetcher<V>,
  ): Effect.Effect<GetOrSetResult<V>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;

    return Effect.gen(function* () {
      const value = yield* runtime.fetchValue(call.key, fetcher, { signal: call.options.signal });

      let outcome: CacheOutcome = CacheOutcome.MISS_LEADER;
      let cached = false;

      if (call.options.shouldCache(value)) {
        yield* runtime.cacheSet(call.key, value, call.options.cacheTtl);
        cached = true;
      } else {
        outcome = CacheOutcome.MISS_LEADER_NOCACHE;
      }

      yield* runtime.leaseMarkReady(call.key);
      yield* runtime.hookOnLeader(call, value, lease.leaseUntil, cached);

      return {
        value,
        meta: CacheLockingRuntime.buildMeta(outcome, lease.leaseUntil),
      } satisfies GetOrSetResult<V>;
    });
  }

  private runFollower(
    call: CallContext<V>,
    lease: LeaseAcquireResult,
    fetcher: Fetcher<V>,
  ): Effect.Effect<GetOrSetResult<V>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;

    return Effect.gen(function* () {
      const waitResult = yield* runtime.waitForCache(call);
      yield* runtime.hookOnFollowerWait(call, lease.leaseUntil, waitResult.waited, waitResult.outcome);

      if (Option.isSome(waitResult.value)) {
        return {
          value: waitResult.value.value,
          meta: CacheLockingRuntime.buildMeta(CacheOutcome.MISS_FOLLOWER_HIT, lease.leaseUntil, waitResult.waited),
        } satisfies GetOrSetResult<V>;
      }

      const fallbackValue = yield* runtime.fetchValue(call.key, fetcher, { signal: call.options.signal });
      yield* runtime.hookOnFallback(call, fallbackValue, lease.leaseUntil, waitResult.waited);

      return {
        value: fallbackValue,
        meta: CacheLockingRuntime.buildMeta(CacheOutcome.MISS_FOLLOWER_FALLBACK, lease.leaseUntil, waitResult.waited),
      } satisfies GetOrSetResult<V>;
    });
  }

  private runLeaderWithRelease(
    call: CallContext<V>,
    lease: LeaseAcquireResult,
    fetcher: Fetcher<V>,
  ): Effect.Effect<GetOrSetResult<V>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;

    return Effect.scoped(
      Effect.acquireRelease(Effect.succeed(lease), () =>
        runtime.leaseRelease(call.key, call.options.ownerId).pipe(
          Effect.catchTags({
            VALIDATION_ERROR: () => Effect.succeed(undefined),
            CACHE_GET_FAILED: () => Effect.succeed(undefined),
            CACHE_SET_FAILED: () => Effect.succeed(undefined),
            LEASE_ACQUIRE_FAILED: () => Effect.succeed(undefined),
            LEASE_RELEASE_FAILED: () => Effect.succeed(undefined),
            LEASE_READY_FAILED: () => Effect.succeed(undefined),
            FETCHER_FAILED: () => Effect.succeed(undefined),
            HOOK_FAILED: () => Effect.succeed(undefined),
            WAIT_STRATEGY_FAILED: () => Effect.succeed(undefined),
            WAIT_FAILED: () => Effect.succeed(undefined),
            ABORTED: () => Effect.succeed(undefined),
          }),
        ),
      ).pipe(Effect.flatMap(() => runtime.runLeader(call, lease, fetcher))),
    );
  }
}
