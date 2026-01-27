import { Effect, Layer } from 'effect';
import type { Cache, CacheLockingEffect, CacheLockingOptions, Fetcher, GetOrSetOptions } from '@core/types';
import type { CacheLockingError } from '@core/errors';
import type { CacheLockingEnv } from '@core/services';
import { CacheService, createCacheLockingLayer, LeasesService } from '@core/services';
import { CacheLockingRuntime } from '@core/runtime';

export class CacheLockingService extends Effect.Service<CacheLockingService>()('CacheLockingService', {
  accessors: true,
  effect: Effect.gen(function* () {
    const runtime = new CacheLockingRuntime<unknown>();
    const env = yield* Effect.context<CacheLockingEnv>();
    const cache = yield* CacheService;
    const leases = yield* LeasesService;
    const getOrSet = Effect.fn('CacheLockingService.getOrSet')(function* (
      key: string,
      fetcher: Fetcher<unknown>,
      options?: GetOrSetOptions<unknown>,
    ) {
      return yield* runtime.getOrSetEffect(key, fetcher, options).pipe(Effect.provide(env));
    });

    return { getOrSet, cache, leases };
  }),
}) {}

export const makeCacheLockingService = <V>(): Effect.Effect<CacheLockingEffect<V>, never, CacheLockingEnv> =>
  Effect.gen(function* () {
    const runtime = new CacheLockingRuntime<V>();
    const env = yield* Effect.context<CacheLockingEnv>();
    const cache = (yield* CacheService) as Cache<V>;
    const leases = yield* LeasesService;

    const getOrSet = Effect.fn('CacheLockingService.getOrSet')(function* (
      key: string,
      fetcher: Fetcher<V>,
      options?: GetOrSetOptions<V>,
    ) {
      return yield* runtime.getOrSetEffect(key, fetcher, options).pipe(Effect.provide(env));
    });

    return { getOrSet, cache, leases };
  });

export const CacheLockingLive: Layer.Layer<CacheLockingService, never, CacheLockingEnv> = CacheLockingService.Default;

export const createCacheLockingServiceLayer = <V>(
  options: CacheLockingOptions<V>,
): Layer.Layer<CacheLockingService, CacheLockingError> =>
  Layer.unwrapEffect(
    createCacheLockingLayer(options).pipe(Effect.map((layer) => Layer.provide(CacheLockingLive, layer))),
  );

export const getCacheLocking = <V>(): Effect.Effect<CacheLockingEffect<V>, never, CacheLockingService> =>
  CacheLockingService.pipe(Effect.map((service) => service as CacheLockingEffect<V>));
