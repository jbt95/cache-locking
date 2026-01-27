import { describe, expect, it } from 'vitest';
import { createCacheLocking } from '@core/factory';
import { CacheOutcome, type CoreClock } from '@core/types';
import { MemoryCache, MemoryLeases } from '@adapters/memory';

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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

describe('createCacheLocking', () => {
  it('returns cache hit without acquiring a lease', async () => {
    const cache = {
      get: async () => 'cached',
      set: async () => {},
    };
    let acquireCalls = 0;
    const leases = {
      acquire: async () => {
        acquireCalls += 1;
        return { role: 'leader' as const, leaseUntil: Date.now() + 1000 };
      },
      release: async () => {},
    };
    const locking = await createCacheLocking({ adapter: { cache, leases } });
    const result = await locking.getOrSet('k', async () => 'miss');

    expect(locking.cache).toBe(cache);
    expect(locking.leases).toBe(leases);
    expect(result.meta.cache).toBe(CacheOutcome.HIT);
    expect(acquireCalls).toBe(0);
  });

  it('fails when adapter does not provide leases', async () => {
    const cache = new MemoryCache<string>();

    await expect(createCacheLocking({ adapter: { cache } })).rejects.toMatchObject({
      _tag: 'VALIDATION_ERROR',
    });
  });

  it('caches on leader path and releases the lease', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const locking = await createCacheLocking({
      adapter: { cache, leases },
      leaseTtl: 1000,
    });

    const result = await locking.getOrSet('k', async () => 'value', { cacheTtl: 5000 });

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER);
    expect(await cache.get('k')).toBe('value');

    const afterRelease = await leases.acquire('k', 'other', 1000);
    expect(afterRelease.role).toBe('leader');
  });

  it('followers wait and hit cache once the leader stores', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const locking = await createCacheLocking({
      adapter: { cache, leases },
      leaseTtl: 1000,
      waitMax: 200,
      waitStep: 10,
    });

    const gate = deferred<void>();
    let fetchCount = 0;

    const leaderPromise = locking.getOrSet('k', async () => {
      fetchCount += 1;
      await gate.promise;
      return 'value';
    });

    await wait(5);

    const followerPromise = locking.getOrSet('k', async () => {
      fetchCount += 1;
      return 'fallback';
    });

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
    const locking = await createCacheLocking({
      adapter: { cache, leases },
      leaseTtl: 1000,
      waitMax: 50,
      waitStep: 10,
      shouldCache: () => false,
    });

    const gate = deferred<void>();
    let fetchCount = 0;

    const leaderPromise = locking.getOrSet('k', async () => {
      fetchCount += 1;
      await gate.promise;
      return 'value';
    });

    await wait(5);

    const followerPromise = locking.getOrSet('k', async () => {
      fetchCount += 1;
      return 'fallback';
    });

    gate.resolve();

    const follower = await followerPromise;
    await leaderPromise;

    expect(fetchCount).toBe(2);
    expect(follower.meta.cache).toBe(CacheOutcome.MISS_FOLLOWER_FALLBACK);
  });

  it('deduplicates concurrent calls for the same key', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const locking = await createCacheLocking({
      adapter: { cache, leases },
      leaseTtl: 1000,
      waitMax: 500,
      waitStep: 10,
    });

    let fetchCount = 0;
    const fetcher = async () => {
      fetchCount += 1;
      await wait(5);
      return 'value';
    };

    const results = await Promise.all(
      Array.from({ length: 10 }, () => locking.getOrSet('k', fetcher, { cacheTtl: 1000 })),
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
    await leases.acquire('k', 'owner-1', 10);
    clock.advance(20);

    const locking = await createCacheLocking({
      adapter: { cache, leases },
      clock,
      sleep: async () => {},
    });

    const result = await locking.getOrSet('k', async () => 'value', { ownerId: 'owner-2' });

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER);
  });

  it('returns MISS_LEADER_NOCACHE when shouldCache is false', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const locking = await createCacheLocking({
      adapter: { cache, leases },
      leaseTtl: 1000,
      shouldCache: () => false,
    });

    const result = await locking.getOrSet('k', async () => 'value', { cacheTtl: 5000 });

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER_NOCACHE);
    expect(await cache.get('k')).toBeNull();
  });

  it('aborts when the signal is already aborted', async () => {
    const cache = new MemoryCache<string>();
    const leases = new MemoryLeases();
    const locking = await createCacheLocking({ adapter: { cache, leases } });
    const controller = new AbortController();
    controller.abort(new Error('stop'));

    await expect(locking.getOrSet('k', async () => 'value', { signal: controller.signal })).rejects.toMatchObject({
      _tag: 'ABORTED',
    });
  });
});
