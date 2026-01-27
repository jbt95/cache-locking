import { expect, it } from 'vitest';
import { CacheOutcome, createCacheLocking } from '@/index';
import { describeIntegration } from './integration-helpers';

describeIntegration('memory adapter integration', () => {
  it('uses memory adapter for cache + leases', async () => {
    const locking = await createCacheLocking<string>({
      adapter: { type: 'memory' },
      leaseTtl: 50,
      waitMax: 100,
      waitStep: 10,
    });

    const result = await locking.getOrSet('memory-key', async () => 'value', { cacheTtl: 500 });

    expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER);
    expect(await locking.cache.get('memory-key')).toBe('value');

    const lease = await locking.leases.acquire('memory-key', 'owner', 50);
    expect(lease.role).toBe('leader');
  });
});
