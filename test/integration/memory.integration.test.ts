import { Effect } from 'effect';
import { expect, it } from 'vitest';
import { Cache, CacheOutcome } from '@/index';
import { describeIntegration } from './integration-helpers';

describeIntegration('memory adapter integration', () => {
  it('uses memory adapter for cache + leases', async () => {
    const adapter = { type: 'memory' } as const;

    const first = await Effect.runPromise(
      Cache.getOrSet({
        adapter,
        key: 'memory-key',
        leaseTtl: 50,
        waitMax: 100,
        waitStep: 10,
        cacheTtl: 500,
        fetcher: () => Effect.succeed('value'),
      }),
    );

    const second = await Effect.runPromise(
      Cache.getOrSet({
        adapter,
        key: 'memory-key',
        leaseTtl: 50,
        waitMax: 100,
        waitStep: 10,
        cacheTtl: 500,
        fetcher: () => Effect.succeed('should-not-run'),
      }),
    );

    expect(first.meta.cache).toBe(CacheOutcome.MISS_LEADER);
    expect(second.meta.cache).toBe(CacheOutcome.HIT);
    expect(second.value).toBe('value');
  });
});
