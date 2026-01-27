import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration } from 'effect';
import { RedisCache, RedisLeases } from '@/index';
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

  it(
    'stores values and respects TTL',
    async () => {
      const cache = new RedisCache<string>({ client: redis.client, keyPrefix: `${prefix}cache:` });

      await cache.set('k', 'value', Duration.millis(300));

      expect(await cache.get('k')).toBe('value');
      expect(await redis.client.pTTL(`${prefix}cache:k`)).toBeGreaterThan(0);

      await wait(350);
      expect(await cache.get('k')).toBeNull();
    },
    10000,
  );

  it(
    'acquires, marks ready, and releases leases',
    async () => {
      const leases = new RedisLeases({ client: redis.client, keyPrefix: `${prefix}lease:` });

      const first = await leases.acquire('k', 'owner-1', Duration.seconds(1));
      expect(first.role).toBe('leader');

      const second = await leases.acquire('k', 'owner-2', Duration.seconds(1));
      expect(second.role).toBe('follower');

      await leases.markReady?.('k');
      const ready = await leases.isReady?.('k');
      expect(ready?.ready).toBe(true);

      await leases.release('k', 'owner-1');
      const third = await leases.acquire('k', 'owner-3', Duration.seconds(1));
      expect(third.role).toBe('leader');
    },
    10000,
  );
});
