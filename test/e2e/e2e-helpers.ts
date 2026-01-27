import { CacheOutcome, getOrSetResponse, type CacheLocking, type ResponseLike } from '@/index';
import { expect, vi } from 'vitest';

type FullPathOptions = {
  locking: CacheLocking<ResponseLike>;
  cacheKeyPrefix?: string;
  cacheTtl?: number;
  request?: { path: string };
};

export const runFullPathE2E = async ({
  locking,
  cacheKeyPrefix = 'http:',
  cacheTtl = 2000,
  request = { path: '/health' },
}: FullPathOptions): Promise<void> => {
  const keyFromRequest = (req: { path: string }) => `path:${req.path}`;
  const cacheKey = `${cacheKeyPrefix ?? ''}${keyFromRequest(request)}`;
  const defaultHeaders = { 'X-Request': 'true' };
  const cacheControl = 'public, max-age=60';

  const fetcher = vi.fn(async () => ({
    status: 200,
    headers: { 'Content-Type': 'text/plain' },
    body: 'ok',
  }));

  const result = await getOrSetResponse({
    getOrSet: locking.getOrSet,
    request,
    keyFromRequest,
    fetcher,
    cacheKeyPrefix,
    cacheControl,
    defaultHeaders,
    cacheTtl,
  });

  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(result.meta.cache).toBe(CacheOutcome.MISS_LEADER);
  expect(result.value.headers).toMatchObject({
    'Content-Type': 'text/plain',
    'Cache-Control': cacheControl,
    'X-Request': 'true',
  });

  const cached = await locking.cache.get(cacheKey);
  expect(cached?.body).toBe('ok');

  const secondFetcher = vi.fn(async () => ({
    status: 200,
    headers: { 'Content-Type': 'text/plain' },
    body: 'should-not-run',
  }));

  const second = await getOrSetResponse({
    getOrSet: locking.getOrSet,
    request,
    keyFromRequest,
    fetcher: secondFetcher,
    cacheKeyPrefix,
    cacheControl,
    defaultHeaders,
    cacheTtl,
  });

  expect(secondFetcher).not.toHaveBeenCalled();
  expect(second.meta.cache).toBe(CacheOutcome.HIT);
};
