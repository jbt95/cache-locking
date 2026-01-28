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
import { AbortedError, FetcherFailed, WaitFailed } from '@core/errors';
import { HookRunner } from '@core/hooks';
import { Phase } from '@core/phases';
import { PhaseRunner } from '@core/phase-runner';
import type { CacheLockingConfig } from '@core/services';
import { CacheLockingConfigService, type CacheLockingEnv, CacheService, LeasesService } from '@core/services';

class CallContext<V, EBase = never, RBase = never, EOverride = never, ROverride = never> {
  constructor(
    public readonly key: Key,
    public readonly options: ResolvedOptions<V>,
    public readonly hooks: HookRunner<V, EBase, RBase, EOverride, ROverride>,
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

/** Core runtime implementing the cache locking flow. */
export class CacheLockingRuntime<V, EBase = never, RBase = never> {
  private readonly phaseRunner = new PhaseRunner();

  getOrSetEffect = <EOverride = never, ROverride = never, EFetcher = never, RFetcher = never>(
    key: string,
    fetcher: Fetcher<V, EFetcher, RFetcher>,
    opts?: GetOrSetOptions<V, EOverride, ROverride>,
  ): Effect.Effect<
    GetOrSetResult<V>,
    CacheLockingError | EBase | EOverride | EFetcher,
    CacheLockingEnv | RBase | ROverride | RFetcher
  > => {
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

  private configEffect(): Effect.Effect<CacheLockingConfig<V, EBase, RBase>, never, CacheLockingEnv> {
    return CacheLockingConfigService.pipe(Effect.map((config) => config as CacheLockingConfig<V, EBase, RBase>));
  }

  private cacheService(): Effect.Effect<Cache<V>, never, CacheLockingEnv> {
    return CacheService.pipe(Effect.map((cache) => cache as Cache<V>));
  }

  private leasesService(): Effect.Effect<Leases, never, CacheLockingEnv> {
    return LeasesService;
  }

  private validateGetOrSetArgsEffect(
    key: string,
    fetcher: Fetcher<V, unknown, unknown>,
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

  private parseCallOptionsEffect<EOverride = never, ROverride = never>(
    key: Key,
    overrides: GetOrSetOptions<V, EOverride, ROverride> | undefined,
    validateOptions: boolean,
  ): Effect.Effect<ValidatedGetOrSetOptions<V, EOverride, ROverride>, CacheLockingError, CacheLockingEnv> {
    if (!validateOptions) {
      return Effect.succeed({
        cacheTtl: resolveOptionalDuration(overrides?.cacheTtl, undefined),
        leaseTtl: resolveOptionalDuration(overrides?.leaseTtl, undefined),
        waitMax: resolveOptionalDuration(overrides?.waitMax, undefined),
        waitStep: resolveOptionalDuration(overrides?.waitStep, undefined),
        shouldCache: overrides?.shouldCache,
        ownerId: overrides?.ownerId as ValidatedGetOrSetOptions<V, EOverride, ROverride>['ownerId'],
        signal: overrides?.signal,
        waitStrategy: overrides?.waitStrategy,
        hooks: overrides?.hooks,
      });
    }

    return decodeWith(callOptionsSchema, overrides ?? {}, 'getOrSet options', {
      key,
      phase: Phase.Validation,
      adapter: 'validation',
    }).pipe(Effect.map((value) => value as ValidatedGetOrSetOptions<V, EOverride, ROverride>));
  }

  private resolveCallOptions<EOverride = never, ROverride = never>(
    defaults: ResolvedDefaults<V, EBase, RBase>,
    parsedOverrides: ValidatedGetOrSetOptions<V, EOverride, ROverride>,
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
    };
  }

  private createCallContextEffect<EOverride = never, ROverride = never>(
    key: Key,
    overrides: GetOrSetOptions<V, EOverride, ROverride> | undefined,
    config: CacheLockingConfig<V, EBase, RBase>,
  ): Effect.Effect<CallContext<V, EBase, RBase, EOverride, ROverride>, CacheLockingError, CacheLockingEnv> {
    const runtime = this;

    return Effect.gen(function* () {
      const parsedOverrides = yield* runtime.parseCallOptionsEffect(key, overrides, config.validateOptions);
      const resolved = runtime.resolveCallOptions(config.defaults, parsedOverrides);
      const hooks = new HookRunner(runtime.phaseRunner, key, config.defaults.hooks, parsedOverrides.hooks);
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

  private withAbortSignal<A, E, R>(
    key: Key,
    signal: AbortSignal | undefined,
    effect: Effect.Effect<A, CacheLockingError | E, CacheLockingEnv | R>,
  ): Effect.Effect<A, CacheLockingError | E, CacheLockingEnv | R> {
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
      const value = yield* runtime.phaseRunner.runEffect(
        Phase.CacheGet,
        { key },
        `cache.get failed for key "${key}"`,
        cache.get(key),
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
      return yield* runtime.phaseRunner.runEffect(
        Phase.CacheSet,
        { key },
        `cache.set failed for key "${key}"`,
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
      return yield* runtime.phaseRunner.runEffect(
        Phase.LeaseAcquire,
        { key },
        `leases.acquire failed for key "${key}"`,
        leases.acquire(key, owner, ttl),
      );
    });
  }

  private leaseRelease(key: Key, owner: OwnerId): Effect.Effect<void, CacheLockingError, CacheLockingEnv> {
    const runtime = this;
    return Effect.gen(function* () {
      const leases = yield* runtime.leasesService();
      return yield* runtime.phaseRunner.runEffect(
        Phase.LeaseRelease,
        { key },
        `leases.release failed for key "${key}"`,
        leases.release(key, owner),
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
      return yield* runtime.phaseRunner.runEffect(
        Phase.LeaseMarkReady,
        { key },
        `leases.markReady failed for key "${key}"`,
        markReady(key),
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
      const state = yield* runtime.phaseRunner.runEffect(
        Phase.LeaseIsReady,
        { key },
        `leases.isReady failed for key "${key}"`,
        isReady(key),
      );
      return Option.fromNullable(state);
    });
  }

  private fetchValue<EFetcher, RFetcher>(
    key: Key,
    fetcher: Fetcher<V, EFetcher, RFetcher>,
    context: FetcherContext,
  ): Effect.Effect<V, CacheLockingError | EFetcher, CacheLockingEnv | RFetcher> {
    const message = `fetcher failed for key "${key}"`;
    const errorContext = { key, phase: Phase.Fetcher, adapter: 'fetcher' } as const;

    return Effect.try({
      try: () => fetcher(context),
      catch: (cause) => new FetcherFailed(message, errorContext, cause),
    }).pipe(
      Effect.flatMap((result) => {
        if (!Effect.isEffect(result)) {
          return Effect.fail(new FetcherFailed(`${message}; fetcher must return an Effect`, errorContext, result));
        }
        return this.phaseRunner.runEffect(Phase.Fetcher, { key }, message, result);
      }),
    );
  }

  private hookOnHit<EOverride, ROverride>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    value: V,
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    return call.hooks.onHit(value, { key: call.key });
  }

  private hookOnLeader<EOverride, ROverride>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    value: V,
    leaseUntil: number,
    cached: boolean,
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    return call.hooks.onLeader(value, { key: call.key, leaseUntil, cached });
  }

  private hookOnFollowerWait<EOverride, ROverride>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    leaseUntil: number,
    waited: Duration.Duration,
    outcome: 'HIT' | 'FALLBACK',
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    return call.hooks.onFollowerWait({
      key: call.key,
      leaseUntil,
      waited,
      outcome,
    });
  }

  private hookOnFallback<EOverride, ROverride>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    value: V,
    leaseUntil: number,
    waited: Duration.Duration,
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    return call.hooks.onFallback(value, { key: call.key, leaseUntil, waited });
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

  private waitForCache<EOverride, ROverride>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
  ): Effect.Effect<WaitOutcome<V>, CacheLockingError, CacheLockingEnv> {
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
        Effect.catchTag('WAIT_RETRY', (error) =>
          Effect.fail(
            new WaitFailed(
              'wait retry escaped retry schedule',
              { key: call.key, phase: Phase.WaitSleep, adapter: 'wait' },
              error,
            ),
          ),
        ),
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

  private runLeader<EOverride, ROverride, EFetcher, RFetcher>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    lease: LeaseAcquireResult,
    fetcher: Fetcher<V, EFetcher, RFetcher>,
  ): Effect.Effect<
    GetOrSetResult<V>,
    CacheLockingError | EBase | EOverride | EFetcher,
    CacheLockingEnv | RBase | ROverride | RFetcher
  > {
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

  private runFollower<EOverride, ROverride, EFetcher, RFetcher>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    lease: LeaseAcquireResult,
    fetcher: Fetcher<V, EFetcher, RFetcher>,
  ): Effect.Effect<
    GetOrSetResult<V>,
    CacheLockingError | EBase | EOverride | EFetcher,
    CacheLockingEnv | RBase | ROverride | RFetcher
  > {
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

  private runLeaderWithRelease<EOverride, ROverride, EFetcher, RFetcher>(
    call: CallContext<V, EBase, RBase, EOverride, ROverride>,
    lease: LeaseAcquireResult,
    fetcher: Fetcher<V, EFetcher, RFetcher>,
  ): Effect.Effect<
    GetOrSetResult<V>,
    CacheLockingError | EBase | EOverride | EFetcher,
    CacheLockingEnv | RBase | ROverride | RFetcher
  > {
    const runtime = this;

    return Effect.scoped(
      Effect.acquireRelease(Effect.succeed(lease), () =>
        runtime.leaseRelease(call.key, call.options.ownerId).pipe(Effect.ignore),
      ).pipe(Effect.flatMap(() => runtime.runLeader(call, lease, fetcher))),
    );
  }
}
