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
  AdapterInput,
  Cache,
  CacheLockingOptions,
  CoreClock,
  Leases,
  ResolvedDefaults,
  Sleep,
  ValidatedCacheLockingOptions,
} from '@core/types';

/** Resolved cache locking defaults and validation flag. */
export type CacheLockingConfig<V, E = never, R = never> = {
  readonly defaults: ResolvedDefaults<V, E, R>;
  readonly validateOptions: boolean;
};

/** Cache service tag for dependency injection. */
export class CacheService extends Context.Tag('CacheService')<CacheService, Cache<unknown>>() {}
/** Leases service tag for dependency injection. */
export class LeasesService extends Context.Tag('LeasesService')<LeasesService, Leases>() {}
/** Cache locking config service tag for dependency injection. */
export class CacheLockingConfigService extends Context.Tag('CacheLockingConfigService')<
  CacheLockingConfigService,
  CacheLockingConfig<unknown, unknown, unknown>
>() {}

/** Full environment required by cache locking runtime. */
export type CacheLockingEnv = CacheService | LeasesService | CacheLockingConfigService | Clock.Clock;

const createClockService = (clock: CoreClock, sleep: Sleep): Clock.Clock => ({
  [Clock.ClockTypeId]: Clock.ClockTypeId,
  unsafeCurrentTimeMillis: (): number => clock.now(),
  currentTimeMillis: Effect.sync(() => clock.now()),
  unsafeCurrentTimeNanos: (): bigint => BigInt(clock.now()) * BigInt(1_000_000),
  currentTimeNanos: Effect.sync(() => BigInt(clock.now()) * BigInt(1_000_000)),
  sleep: (duration: EffectDuration) => sleep(duration),
});

const validationContext = { phase: Phase.Validation, adapter: 'validation' } as const;

const isProviderAdapter = <V>(value: unknown): value is ProviderAdapter<V> =>
  typeof value === 'object' && value !== null && 'cache' in value;

const isAdapterConfig = <V>(value: unknown): value is AdapterConfig<V> =>
  typeof value === 'object' && value !== null && 'type' in value;

const isAdapterAlias = (value: unknown): value is 'memory' => value === 'memory';

const adapterCache = new WeakMap<AdapterConfig<unknown>, ProviderAdapter<unknown>>();
const sharedMemoryAdapter = createAdapter<unknown>({ type: 'memory' });

const resolveAdapterConfig = <V>(
  adapter: AdapterConfig<V>,
  label: string,
): Effect.Effect<ProviderAdapter<V>, CacheLockingError> => {
  if (adapter.type === 'memory' && adapter.options === undefined) {
    return Effect.succeed(sharedMemoryAdapter as ProviderAdapter<V>);
  }
  const cached = adapterCache.get(adapter as AdapterConfig<unknown>);
  if (cached) {
    return Effect.succeed(cached as ProviderAdapter<V>);
  }
  return Effect.try({
    try: () => {
      const created = createAdapter(adapter);
      adapterCache.set(adapter as AdapterConfig<unknown>, created as ProviderAdapter<unknown>);
      return created;
    },
    catch: (cause) => new ValidationError(`${label} creation failed`, validationContext, undefined, cause),
  });
};

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

  if (isAdapterAlias(adapter)) {
    return Effect.succeed(sharedMemoryAdapter as ProviderAdapter<V>);
  }

  if (isAdapterConfig(adapter)) {
    return resolveAdapterConfig(adapter, label);
  }

  return Effect.fail(
    new ValidationError(`${label} must be "memory", an adapter config, or an adapter instance`, validationContext),
  );
};

const resolveLeases = <V, E = never, R = never>(
  options: CacheLockingOptions<V, E, R>,
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

const coerceOptions = <V, E = never, R = never>(
  options: CacheLockingOptions<V, E, R>,
  cache: Cache<V>,
  leases: Leases,
): ValidatedCacheLockingOptions<V, E, R> => ({
  cache,
  leases,
  clock: options.clock ?? defaultClock,
  sleep: options.sleep ?? defaultSleep,
  shouldCache: options.shouldCache ?? defaultShouldCache,
  ownerId: options.ownerId as ValidatedCacheLockingOptions<V, E, R>['ownerId'],
  leaseTtl: resolveOptionalDuration(options.leaseTtl, undefined),
  waitMax: resolveOptionalDuration(options.waitMax, undefined),
  waitStep: resolveOptionalDuration(options.waitStep, undefined),
  cacheTtl: resolveOptionalDuration(options.cacheTtl, undefined),
  signal: options.signal,
  waitStrategy: options.waitStrategy ?? defaultWaitStrategy,
  hooks: options.hooks,
  validateOptions: options.validateOptions,
});

/** Resolve option defaults with fallbacks applied. */
export const resolveDefaults = <V, E = never, R = never>(
  options: ValidatedCacheLockingOptions<V, E, R>,
): ResolvedDefaults<V, E, R> => ({
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

const validateBaseOptions = <V, E = never, R = never>(
  options: CacheLockingOptions<V, E, R>,
): Effect.Effect<CacheLockingOptions<V, E, R>, CacheLockingError> =>
  decodeWith(baseOptionsSchema, options, 'cache-locking options', validationContext).pipe(Effect.as(options));

/** Validate and normalize cache locking options. */
export const normalizeCacheLockingOptions = <V, E = never, R = never>(
  options: CacheLockingOptions<V, E, R>,
): Effect.Effect<ValidatedCacheLockingOptions<V, E, R>, CacheLockingError> => {
  if (!options) {
    return Effect.fail(new ValidationError('cache-locking options are required', validationContext));
  }

  const baseEffect = options.validateOptions === false ? Effect.succeed(options) : validateBaseOptions(options);

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

const createCacheLockingLayerFromValidated = <V, E = never, R = never>(
  validated: ValidatedCacheLockingOptions<V, E, R>,
): Layer.Layer<CacheLockingEnv> => {
  const defaults = resolveDefaults(validated);

  const clock = validated.clock ?? defaultClock;
  const sleep = validated.sleep ?? defaultSleep;
  const clockLayer = Layer.succeed(Clock.Clock, createClockService(clock, sleep));

  const cacheLayer = Layer.succeed(CacheService, validated.cache as Cache<unknown>);
  const leasesLayer = Layer.succeed(LeasesService, validated.leases);
  const configLayer = Layer.succeed(CacheLockingConfigService, {
    defaults: defaults as ResolvedDefaults<unknown, unknown, unknown>,
    validateOptions: validated.validateOptions !== false,
  });

  return Layer.mergeAll(cacheLayer, leasesLayer, configLayer, clockLayer);
};

/** Create a layer and return resolved cache and leases. */
export const createCacheLockingLayerFromOptions = <V, E = never, R = never>(
  options: CacheLockingOptions<V, E, R>,
): Effect.Effect<{ layer: Layer.Layer<CacheLockingEnv>; cache: Cache<V>; leases: Leases }, CacheLockingError> =>
  Effect.gen(function* () {
    const validated = yield* normalizeCacheLockingOptions(options);
    return {
      layer: createCacheLockingLayerFromValidated(validated),
      cache: validated.cache,
      leases: validated.leases,
    };
  });
