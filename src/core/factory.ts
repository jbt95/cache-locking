import { Effect } from 'effect';
import type { CacheLockingOptions, GetOrSetOnceOptions, GetOrSetResult } from '@core/types';
import type { CacheLockingError } from '@core/errors';
import { CacheLockingRuntime } from '@core/runtime';
import { createCacheLockingLayerFromOptions } from '@core/services';

/**
 * Get or set a value in the cache with cache locking.
 *
 * If the value is not present in the cache, the fetcher function is called to retrieve it.
 * Cache locking ensures that only one fetcher is executed for concurrent requests for the same key.
 * Other requests will wait for the fetcher to complete and then return the cached value.
 *
 * @param options - The options for getting or setting the value.
 * @returns An effect that resolves to the cached or fetched value.
 * */
export const getOrSet = <V, EFetcher = never, RFetcher = never, EHooks = never, RHooks = never>(
  options: GetOrSetOnceOptions<V, EFetcher, RFetcher, EHooks, RHooks>,
): Effect.Effect<GetOrSetResult<V>, CacheLockingError | EFetcher | EHooks, RFetcher | RHooks> =>
  Effect.gen(function* () {
    const { key, fetcher, ...cacheLockingOptions } = options;
    const { layer } = yield* createCacheLockingLayerFromOptions(
      cacheLockingOptions as CacheLockingOptions<V, EHooks, RHooks>,
    );
    const runtime = new CacheLockingRuntime<V, EHooks, RHooks>();
    return yield* runtime.getOrSetEffect(key, fetcher).pipe(Effect.provide(layer));
  });
