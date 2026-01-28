import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';
import { Cache } from '@/index';
import { CacheOutcome, type CoreClock } from '@core/types';
import { MemoryCache, MemoryLeases } from '@adapters/memory';

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const captureError = <A, E>(effect: Effect.Effect<A, E, never>) =>
  Effect.runPromise(effect.pipe(Effect.catchAll((cause) => Effect.succeed(cause))));

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

class ManualClock implements CoreClock {
  private nowMs = 0;
  now() {
    return this.nowMs;
  }
  advance(ms: number) {
    this.nowMs += ms;
  }
}

const gateEffect = <T>(promise: Promise<T>): Effect.Effect<T, never> =>
  Effect.async<T>((resume) => {
    promise.then((value) => resume(Effect.succeed(value)));
    return Effect.sync(() => undefined);
  });

describe('Cache.getOrSet', () => {
  it('returns cache hit without acquiring a lease', async () => {
    const cache = {
      get: (_key: string) => Effect.succeed('cached'),
      set: (_key: string, _value: string, _ttl?: unknown) => Effect.succeed(undefined),
    };
    let acquireCalls = 0;
    const leases = {
      acquire: (_key: string, _owner: string, _ttl: unknown) =>
        Effect.sync(() => {
          acquireCalls += 1;
          return { role: 'leader' as const, leaseUntil: Date.now() + 1000 };
        }),
      release: (_key: string, _owner: string) => Effect.succeed(undefined),
    };
    const result = await Effect.runPromise(
      Cache.getOrSet({
        adapter: { cache, leases },
        key: 'k',
        fetcher: () => Effect.succeed('miss'),
      }),
    );

    expect(result.meta.cache).toBe(CacheOutcome.HIT);
    expect(acquireCalls).toBe(0);
  });

  it('fails when adapter does not provide leases', async () => {
    const cache = new MemoryCache<string>();

    const error = await captureError(
      Cache.getOrSet({
        adapter: { cache },
        key: 'k',
        fetcher: () => Effect.succeed('value'),
      }),
    );

    expect(error).toMatchObject({ _tag: 'VALIDATION_ERROR' });
  });

  it('caches on leader path and releases the lease', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const result = await Effect.runPromise(
      Cache.getOrSet({
        adapter: { cache, leases },
        key: 'k',
        leaseTtl: 1000,
        cacheTtl: 5000,
        fetcher: () => Effect.succeed('value'),
      }),
    );

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER);
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    const afterRelease = await Effect.runPromise(leases.acquire('k', 'other', 1000));
    expect(afterRelease.role).toBe('leader');
  });

  it('followers wait and hit cache once the leader stores', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const followerAcquire = deferred<void>();
    const adapter = {
      cache,
      leases: {
        acquire: (key: string, owner: string, ttl: Parameters<MemoryLeases['acquire']>[2]) =>
          leases.acquire(key, owner, ttl).pipe(
            Effect.tap((result) =>
              Effect.sync(() => {
                if (result.role === 'follower') {
                  followerAcquire.resolve();
                }
              }),
            ),
          ),
        release: leases.release.bind(leases),
        markReady: leases.markReady?.bind(leases),
        isReady: leases.isReady?.bind(leases),
      },
    };
    const gate = deferred<void>();
    const leaderStarted = deferred<void>();
    let fetchCount = 0;

    const leaderPromise = Effect.runPromise(
      Cache.getOrSet({
        adapter,
        key: 'k',
        leaseTtl: 1000,
        waitMax: 200,
        waitStep: 10,
        fetcher: () =>
          Effect.gen(function* () {
            yield* Effect.sync(() => {
              fetchCount += 1;
              leaderStarted.resolve();
            });
            yield* gateEffect(gate.promise);
            return 'value';
          }),
      }),
    );

    await leaderStarted.promise;

    const followerPromise = Effect.runPromise(
      Cache.getOrSet({
        adapter,
        key: 'k',
        leaseTtl: 1000,
        waitMax: 200,
        waitStep: 10,
        fetcher: () =>
          Effect.sync(() => {
            fetchCount += 1;
            return 'fallback';
          }),
      }),
    );

    await followerAcquire.promise;
    gate.resolve();

    const leader = await leaderPromise;
    const follower = await followerPromise;

    expect(fetchCount).toBe(1);
    expect(leader.meta.cache).toBe(CacheOutcome.MISS_LEADER);
    expect(follower.meta.cache).toBe(CacheOutcome.MISS_FOLLOWER_HIT);
  });

  it('falls back when the leader does not cache', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const followerAcquire = deferred<void>();
    const adapter = {
      cache,
      leases: {
        acquire: (key: string, owner: string, ttl: Parameters<MemoryLeases['acquire']>[2]) =>
          leases.acquire(key, owner, ttl).pipe(
            Effect.tap((result) =>
              Effect.sync(() => {
                if (result.role === 'follower') {
                  followerAcquire.resolve();
                }
              }),
            ),
          ),
        release: leases.release.bind(leases),
        markReady: leases.markReady?.bind(leases),
        isReady: leases.isReady?.bind(leases),
      },
    };
    const gate = deferred<void>();
    const leaderStarted = deferred<void>();
    let fetchCount = 0;

    const leaderPromise = Effect.runPromise(
      Cache.getOrSet({
        adapter,
        key: 'k',
        leaseTtl: 1000,
        waitMax: 50,
        waitStep: 10,
        shouldCache: () => false,
        fetcher: () =>
          Effect.gen(function* () {
            yield* Effect.sync(() => {
              fetchCount += 1;
              leaderStarted.resolve();
            });
            yield* gateEffect(gate.promise);
            return 'value';
          }),
      }),
    );

    await leaderStarted.promise;

    const followerPromise = Effect.runPromise(
      Cache.getOrSet({
        adapter,
        key: 'k',
        leaseTtl: 1000,
        waitMax: 50,
        waitStep: 10,
        shouldCache: () => false,
        fetcher: () =>
          Effect.sync(() => {
            fetchCount += 1;
            return 'fallback';
          }),
      }),
    );

    await followerAcquire.promise;
    gate.resolve();

    const follower = await followerPromise;
    await leaderPromise;

    expect(fetchCount).toBe(2);
    expect(follower.meta.cache).toBe(CacheOutcome.MISS_FOLLOWER_FALLBACK);
  });

  it('deduplicates concurrent calls for the same key', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const adapter = { cache, leases };
    let fetchCount = 0;
    const fetcher = () =>
      Effect.gen(function* () {
        fetchCount += 1;
        yield* gateEffect(wait(5).then(() => undefined));
        return 'value';
      });

    const results = await Promise.all(
      Array.from({ length: 10 }, () =>
        Effect.runPromise(
          Cache.getOrSet({
            adapter,
            key: 'k',
            leaseTtl: 1000,
            waitMax: 500,
            waitStep: 10,
            cacheTtl: 1000,
            fetcher,
          }),
        ),
      ),
    );

    expect(fetchCount).toBe(1);
    results.forEach((result) => {
      expect(result.value).toBe('value');
    });
  });

  it('can become leader after lease expiry', async () => {
    const clock = new ManualClock();
    const cache = new MemoryCache<string>({ clock });
    const leases = new MemoryLeases({ clock });
    await Effect.runPromise(leases.acquire('k', 'owner-1', 10));
    clock.advance(20);

    const result = await Effect.runPromise(
      Cache.getOrSet({
        adapter: { cache, leases },
        key: 'k',
        ownerId: 'owner-2',
        fetcher: () => Effect.succeed('value'),
      }),
    );

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER);
  });

  it('returns MISS_LEADER_NOCACHE when shouldCache is false', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const result = await Effect.runPromise(
      Cache.getOrSet({
        adapter: { cache, leases },
        key: 'k',
        leaseTtl: 1000,
        shouldCache: () => false,
        cacheTtl: 5000,
        fetcher: () => Effect.succeed('value'),
      }),
    );

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER_NOCACHE);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();
  });

  it('aborts when the signal is already aborted', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const controller = new AbortController();
    controller.abort(new Error('stop'));

    const error = await captureError(
      Cache.getOrSet({
        adapter: { cache, leases },
        key: 'k',
        signal: controller.signal,
        fetcher: () => Effect.succeed('value'),
      }),
    );

    expect(error).toMatchObject({ _tag: 'ABORTED' });
  });
});
