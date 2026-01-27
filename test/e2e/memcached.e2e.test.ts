import { afterAll, beforeAll, it } from 'vitest';
import { MemoryLeases, createAdapter, createCacheLocking, type ResponseLike } from '@/index';
import { startMemcached, type MemcachedTestContext } from '../support/memcached';
import { describeContainerIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeContainerIntegration('memcached adapter e2e', () => {
  const prefix = makeTestPrefix('memcached-e2e');
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
    'runs the full path with memory leases',
    async () => {
      const adapter = createAdapter<ResponseLike>({
        type: 'memcached',
        options: { client: memcached.client, keyPrefix: `${prefix}cache:` },
      });

      const locking = await createCacheLocking<ResponseLike>({
        adapter,
        leases: new MemoryLeases(),
      });

      await runFullPathE2E({ locking });
    },
    10000,
  );
});
