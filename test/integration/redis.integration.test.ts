import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration, Effect } from 'effect';
import { RedisCache, RedisLeases } from '@adapters/redis';
import { startRedis, type RedisTestContext } from '../support/redis';
import { describeContainerIntegration, makeTestPrefix, wait } from './integration-helpers';

describeContainerIntegration('redis adapter integration', () => {
  const prefix = makeTestPrefix('redis');
  let redis: RedisTestContext;

  beforeAll(async () => {
    redis = await startRedis();
  });

  afterAll(async () => {
    if (redis) {
      await redis.stop();
    }
  });

  it('stores values and respects TTL', async () => {
    const cache = new RedisCache<string>({ client: redis.client, keyPrefix: `${prefix}cache:` });

    await Effect.runPromise(cache.set('k', 'value', Duration.millis(300)));

    expect(await Effect.runPromise(cache.get('k'))).toBe('value');
    expect(await redis.client.pTTL(`${prefix}cache:k`)).toBeGreaterThan(0);

    await wait(350);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();
  }, 10000);

  it('acquires, marks ready, and releases leases', async () => {
    const leases = new RedisLeases({ client: redis.client, keyPrefix: `${prefix}lease:` });

    const first = await Effect.runPromise(leases.acquire('k', 'owner-1', Duration.seconds(1)));
    expect(first.role).toBe('leader');

    const second = await Effect.runPromise(leases.acquire('k', 'owner-2', Duration.seconds(1)));
    expect(second.role).toBe('follower');

    if (leases.markReady) {
      await Effect.runPromise(leases.markReady('k'));
    }
    const ready = leases.isReady ? await Effect.runPromise(leases.isReady('k')) : undefined;
    expect(ready?.ready).toBe(true);

    await Effect.runPromise(leases.release('k', 'owner-1'));
    const third = await Effect.runPromise(leases.acquire('k', 'owner-3', Duration.seconds(1)));
    expect(third.role).toBe('leader');
  }, 10000);
});
