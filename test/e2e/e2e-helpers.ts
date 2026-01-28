import { Effect } from 'effect';
import { Cache, CacheOutcome, type AdapterInput, type Leases } from '@/index';
import { expect, vi } from 'vitest';

type FullPathOptions = {
  adapter: AdapterInput<string>;
  leases?: Leases;
  cacheTtl?: number;
  key?: string;
};

export const runFullPathE2E = async ({
  adapter,
  leases,
  cacheTtl = 2000,
  key = 'health',
}: FullPathOptions): Promise<void> => {
  const fetcher = vi.fn(() => Effect.succeed('ok'));

  const first = await Effect.runPromise(
    Cache.getOrSet({
      adapter,
      leases,
      key,
      cacheTtl,
      fetcher,
    }),
  );

  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(first.meta.cache).toBe(CacheOutcome.MISS_LEADER);

  const secondFetcher = vi.fn(() => Effect.succeed('should-not-run'));
  const second = await Effect.runPromise(
    Cache.getOrSet({
      adapter,
      leases,
      key,
      cacheTtl,
      fetcher: secondFetcher,
    }),
  );

  expect(secondFetcher).not.toHaveBeenCalled();
  expect(second.meta.cache).toBe(CacheOutcome.HIT);
  expect(second.value).toBe('ok');
};
