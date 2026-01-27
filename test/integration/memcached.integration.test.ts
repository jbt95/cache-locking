import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration } from 'effect';
import { MemcachedCache } from '@/index';
import { startMemcached, type MemcachedTestContext } from '../support/memcached';
import { describeContainerIntegration, makeTestPrefix, wait } from './integration-helpers';

describeContainerIntegration('memcached adapter integration', () => {
  const prefix = makeTestPrefix('memcached');
  let memcached: MemcachedTestContext;

  beforeAll(async () => {
    memcached = await startMemcached();
  });

  afterAll(async () => {
    if (memcached) {
      await memcached.stop();
    }
  });

  it(
    'stores values and expires them with TTL',
    async () => {
      const cache = new MemcachedCache<string>({
        client: memcached.client,
        keyPrefix: `${prefix}cache:`,
      });

      await cache.set('k', 'value', Duration.seconds(1));

      expect(await cache.get('k')).toBe('value');
      await wait(1200);
      expect(await cache.get('k')).toBeNull();
    },
    10000,
  );
});
