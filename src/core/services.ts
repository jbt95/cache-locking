import { Clock, Context, Effect, Layer } from 'effect';
import type { Duration as EffectDuration } from 'effect/Duration';
import { baseOptionsSchema, decodeCache, decodeLeases, decodeWith } from '@core/validation';
import {
  createOwnerId,
  DEFAULT_LEASE_TTL,
  DEFAULT_WAIT_MAX,
  DEFAULT_WAIT_STEP,
  defaultClock,
  defaultShouldCache,
  defaultSleep,
  defaultWaitStrategy,
  resolveDuration,
  resolveOptionalDuration,
} from '@core/defaults';
import { Phase } from '@core/phases';
import type { CacheLockingError } from '@core/errors';
import { ValidationError } from '@core/errors';
import { createAdapter, type AdapterConfig, type ProviderAdapter } from '@adapters/factory';
import type {
  Cache,
  CacheLockingOptions,
  CoreClock,
  Leases,
  ResolvedDefaults,
  Sleep,
  ValidatedCacheLockingOptions,
} from '@core/types';

export type CacheLockingConfig<V> = {
  readonly defaults: ResolvedDefaults<V>;
  readonly validateOptions: boolean;
};

export class CacheService extends Context.Tag('CacheService')<CacheService, Cache<unknown>>() {}
export class LeasesService extends Context.Tag('LeasesService')<LeasesService, Leases>() {}
export class CacheLockingConfigService extends Context.Tag('CacheLockingConfigService')<
  CacheLockingConfigService,
  CacheLockingConfig<unknown>
>() {}

export type CacheLockingEnv = CacheService | LeasesService | CacheLockingConfigService | Clock.Clock;

const createClockService = (clock: CoreClock, sleep: Sleep): Clock.Clock => ({
  [Clock.ClockTypeId]: Clock.ClockTypeId,
  unsafeCurrentTimeMillis: (): number => clock.now(),
  currentTimeMillis: Effect.sync(() => clock.now()),
  unsafeCurrentTimeNanos: (): bigint => BigInt(clock.now()) * BigInt(1_000_000),
  currentTimeNanos: Effect.sync(() => BigInt(clock.now()) * BigInt(1_000_000)),
  sleep: (duration: EffectDuration) =>
    Effect.async<void>((resume) => {
      sleep(duration)
        .then(() => resume(Effect.succeed(undefined)))
        .catch((cause) => resume(Effect.die(cause)));
    }),
});

type AdapterInput<V> = AdapterConfig<V> | ProviderAdapter<V>;

const validationContext = { phase: Phase.Validation, adapter: 'validation' } as const;

const isProviderAdapter = <V>(value: unknown): value is ProviderAdapter<V> =>
  typeof value === 'object' && value !== null && 'cache' in value;

const isAdapterConfig = <V>(value: unknown): value is AdapterConfig<V> =>
  typeof value === 'object' && value !== null && 'type' in value;

const resolveAdapter = <V>(
  adapter: AdapterInput<V> | undefined,
  label: string,
): Effect.Effect<ProviderAdapter<V>, CacheLockingError> => {
  if (!adapter) {
    return Effect.fail(new ValidationError(`${label} is required`, validationContext));
  }

  if (isProviderAdapter(adapter)) {
    return Effect.succeed(adapter);
  }

  if (isAdapterConfig(adapter)) {
    return Effect.try({
      try: () => createAdapter(adapter),
      catch: (cause) => new ValidationError(`${label} creation failed`, validationContext, undefined, cause),
    });
  }

  return Effect.fail(new ValidationError(`${label} must be an adapter config or adapter instance`, validationContext));
};

const resolveLeases = <V>(
  options: CacheLockingOptions<V>,
  adapter: ProviderAdapter<V>,
): Effect.Effect<Leases, CacheLockingError> => {
  if (options.leases) {
    return Effect.succeed(options.leases);
  }

  if (adapter.leases) {
    return Effect.succeed(adapter.leases);
  }

  return Effect.fail(
    new ValidationError(
      'leases are required; pass leases explicitly or use an adapter that provides leases',
      validationContext,
    ),
  );
};

const validateResolvedAdapters = <V>(cache: Cache<V>, leases: Leases): Effect.Effect<void, CacheLockingError> =>
  Effect.gen(function* () {
    yield* decodeCache(cache, validationContext);
    yield* decodeLeases(leases, validationContext);
  });

const coerceOptions = <V>(
  options: CacheLockingOptions<V>,
  cache: Cache<V>,
  leases: Leases,
): ValidatedCacheLockingOptions<V> => ({
  cache,
  leases,
  clock: options.clock ?? defaultClock,
  sleep: options.sleep ?? defaultSleep,
  shouldCache: options.shouldCache ?? defaultShouldCache,
  ownerId: options.ownerId as ValidatedCacheLockingOptions<V>['ownerId'],
  leaseTtl: resolveOptionalDuration(options.leaseTtl, undefined),
  waitMax: resolveOptionalDuration(options.waitMax, undefined),
  waitStep: resolveOptionalDuration(options.waitStep, undefined),
  cacheTtl: resolveOptionalDuration(options.cacheTtl, undefined),
  signal: options.signal,
  waitStrategy: options.waitStrategy ?? defaultWaitStrategy,
  hooks: options.hooks,
  validateOptions: options.validateOptions,
});

export const resolveDefaults = <V>(options: ValidatedCacheLockingOptions<V>): ResolvedDefaults<V> => ({
  leaseTtl: resolveDuration(options.leaseTtl, DEFAULT_LEASE_TTL),
  waitMax: resolveDuration(options.waitMax, DEFAULT_WAIT_MAX),
  waitStep: resolveDuration(options.waitStep, DEFAULT_WAIT_STEP),
  shouldCache: options.shouldCache ?? defaultShouldCache,
  cacheTtl: resolveOptionalDuration(options.cacheTtl, undefined),
  ownerId: options.ownerId ?? createOwnerId(),
  signal: options.signal,
  waitStrategy: options.waitStrategy ?? defaultWaitStrategy,
  hooks: options.hooks,
});

const validateBaseOptions = <V>(
  options: CacheLockingOptions<V>,
): Effect.Effect<CacheLockingOptions<V>, CacheLockingError> =>
  decodeWith(baseOptionsSchema, options, 'createCacheLocking options', validationContext).pipe(
    Effect.map((value) => value as CacheLockingOptions<V>),
  );

export const normalizeCacheLockingOptions = <V>(
  options: CacheLockingOptions<V>,
): Effect.Effect<ValidatedCacheLockingOptions<V>, CacheLockingError> => {
  if (!options) {
    return Effect.fail(
      new ValidationError('createCacheLocking options are required', validationContext),
    );
  }

  const baseEffect =
    options.validateOptions === false ? Effect.succeed(options) : validateBaseOptions(options);

  return baseEffect.pipe(
    Effect.flatMap((baseOptions) =>
      Effect.gen(function* () {
        const adapter = yield* resolveAdapter(baseOptions.adapter, 'adapter');
        const leases = yield* resolveLeases(baseOptions, adapter);

        if (baseOptions.validateOptions !== false) {
          yield* validateResolvedAdapters(adapter.cache, leases);
        }

        return coerceOptions(baseOptions, adapter.cache, leases);
      }),
    ),
  );
};

const createCacheLockingLayerFromValidated = <V>(
  validated: ValidatedCacheLockingOptions<V>,
): Layer.Layer<CacheLockingEnv> => {
  const defaults = resolveDefaults(validated);

  const clock = validated.clock ?? defaultClock;
  const sleep = validated.sleep ?? defaultSleep;
  const clockLayer = Layer.succeed(Clock.Clock, createClockService(clock, sleep));

  const cacheLayer = Layer.succeed(CacheService, validated.cache as Cache<unknown>);
  const leasesLayer = Layer.succeed(LeasesService, validated.leases);
  const configLayer = Layer.succeed(CacheLockingConfigService, {
    defaults: defaults as ResolvedDefaults<unknown>,
    validateOptions: validated.validateOptions !== false,
  });

  return Layer.mergeAll(cacheLayer, leasesLayer, configLayer, clockLayer);
};

export const createCacheLockingLayer = <V>(
  options: CacheLockingOptions<V>,
): Effect.Effect<Layer.Layer<CacheLockingEnv>, CacheLockingError> =>
  Effect.gen(function* () {
    const validated = yield* normalizeCacheLockingOptions(options);
    return createCacheLockingLayerFromValidated(validated);
  });

export const createCacheLockingLayerFromOptions = <V>(
  options: CacheLockingOptions<V>,
): Effect.Effect<{ layer: Layer.Layer<CacheLockingEnv>; cache: Cache<V>; leases: Leases }, CacheLockingError> =>
  Effect.gen(function* () {
    const validated = yield* normalizeCacheLockingOptions(options);
    return {
      layer: createCacheLockingLayerFromValidated(validated),
      cache: validated.cache,
      leases: validated.leases,
    };
  });
