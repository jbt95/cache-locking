import { Cause, Effect, Either, Exit, ManagedRuntime } from 'effect';
import type { ManagedRuntime as ManagedRuntimeType } from 'effect/ManagedRuntime';
import type {
  Cache,
  CacheLocking,
  CacheLockingEffect,
  CacheLockingOptions,
  Fetcher,
  GetOrSetEffectFn,
  GetOrSetOptions,
  GetOrSetResult,
  Leases,
} from '@core/types';
import type { CacheLockingError } from '@core/errors';
import { CacheLockingRuntime } from '@core/runtime';
import { type CacheLockingEnv, createCacheLockingLayerFromOptions } from '@core/services';

class CacheLockingClient<V> implements CacheLocking<V> {
  private readonly runtime: CacheLockingRuntime<V>;
  private readonly managedRuntime: ManagedRuntimeType<CacheLockingEnv, CacheLockingError>;
  cache: Cache<V>;
  leases: Leases;

  constructor(
    runtime: CacheLockingRuntime<V>,
    managedRuntime: ManagedRuntimeType<CacheLockingEnv, CacheLockingError>,
    cache: Cache<V>,
    leases: Leases,
  ) {
    this.runtime = runtime;
    this.managedRuntime = managedRuntime;
    this.cache = cache;
    this.leases = leases;
  }

  getOrSet: GetOrSetFn<V> = (key, fetcher, options) =>
    this.managedRuntime.runPromiseExit(this.runtime.getOrSetEffect(key, fetcher, options)).then(unwrapExit);
}

class CacheLockingEffectClient<V> implements CacheLockingEffect<V> {
  private readonly runtime: CacheLockingRuntime<V>;
  private readonly managedRuntime: ManagedRuntimeType<CacheLockingEnv, CacheLockingError>;
  cache: Cache<V>;
  leases: Leases;

  constructor(
    runtime: CacheLockingRuntime<V>,
    managedRuntime: ManagedRuntimeType<CacheLockingEnv, CacheLockingError>,
    cache: Cache<V>,
    leases: Leases,
  ) {
    this.runtime = runtime;
    this.managedRuntime = managedRuntime;
    this.cache = cache;
    this.leases = leases;
  }

  getOrSet: GetOrSetEffectFn<V> = (key: string, fetcher: Fetcher<V>, options?: GetOrSetOptions<V>) =>
    this.runtime.getOrSetEffect(key, fetcher, options).pipe(Effect.provide(this.managedRuntime));
}

type ManagedRuntimeResources<V> = {
  managedRuntime: ManagedRuntimeType<CacheLockingEnv, CacheLockingError>;
  cache: Cache<V>;
  leases: Leases;
};

const unwrapExit = <A, E>(exit: Exit.Exit<A, E>): A => {
  if (Exit.isSuccess(exit)) {
    return exit.value;
  }
  const failureOrCause = Cause.failureOrCause(exit.cause);
  if (Either.isLeft(failureOrCause)) {
    throw failureOrCause.left;
  }
  throw Cause.squash(exit.cause);
};

const runPromiseOrThrow = async <A, E>(effect: Effect.Effect<A, E>): Promise<A> => {
  const exit = await Effect.runPromiseExit(effect);
  return unwrapExit(exit);
};

const buildManagedRuntime = <V>(
  options: CacheLockingOptions<V>,
): Effect.Effect<ManagedRuntimeResources<V>, CacheLockingError> =>
  Effect.gen(function* () {
    const { layer, cache, leases } = yield* createCacheLockingLayerFromOptions(options);
    return { managedRuntime: ManagedRuntime.make(layer), cache, leases };
  });

export const createCacheLockingEffect = <V>(
  options: CacheLockingOptions<V>,
): Effect.Effect<CacheLockingEffect<V>, CacheLockingError> =>
  Effect.gen(function* () {
    const { managedRuntime, cache, leases } = yield* buildManagedRuntime(options);
    const runtime = new CacheLockingRuntime<V>();
    return new CacheLockingEffectClient(runtime, managedRuntime, cache, leases);
  });

export const createCacheLocking = async <V>(options: CacheLockingOptions<V>): Promise<CacheLocking<V>> => {
  const { managedRuntime, cache, leases } = await runPromiseOrThrow(buildManagedRuntime(options));
  const runtime = new CacheLockingRuntime<V>();
  return new CacheLockingClient(runtime, managedRuntime, cache, leases);
};
