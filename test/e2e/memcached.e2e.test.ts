import { afterAll, beforeAll, it } from 'vitest';
import { MemoryLeases } from '@adapters/memory';
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

  it('runs the full path with memory leases', async () => {
    const adapter = {
      type: 'memcached',
      options: { client: memcached.client, keyPrefix: `${prefix}cache:` },
    } as const;

    await runFullPathE2E({ adapter, leases: new MemoryLeases() });
  }, 10000);
});
