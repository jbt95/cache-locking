import { describe, expect, it, vi } from 'vitest';
import type { Fetcher, GetOrSetOptions } from '@core/types';
import { CacheOutcome } from '@core/types';
import { getOrSetResponse, type ResponseLike } from '@core/http/get-or-set-response';

describe('getOrSetResponse', () => {
  it('builds keys, forwards options, and composes headers', async () => {
    const response: ResponseLike = {
      status: 200,
      headers: {
        'Content-Type': 'text/plain',
        'X-Value': '1',
        'Cache-Control': 'max-age=0',
      },
      body: 'ok',
    };
    const getOrSet = vi.fn(async (key: string, fetcher: Fetcher<ResponseLike>, options?: GetOrSetOptions<ResponseLike>) => {
      const value = await fetcher();
      return { value, meta: { cache: CacheOutcome.HIT } };
    });
    const request = { path: '/cats' };

    const result = await getOrSetResponse({
      getOrSet,
      request,
      keyFromRequest: (req) => req.path,
      fetcher: async () => response,
      cacheKeyPrefix: 'api:',
      cacheControl: 'public, max-age=60',
      defaultHeaders: {
        'X-Default': '1',
        'Content-Type': 'application/json',
      },
      cacheTtl: 500,
      leaseTtl: 1000,
      waitMax: 2000,
      waitStep: 50,
      ownerId: 'owner',
    });

    expect(getOrSet).toHaveBeenCalledTimes(1);
    expect(getOrSet.mock.calls[0][0]).toBe('api:/cats');
    expect(getOrSet.mock.calls[0][2]).toMatchObject({
      cacheTtl: 500,
      leaseTtl: 1000,
      waitMax: 2000,
      waitStep: 50,
      ownerId: 'owner',
    });

    expect(result.meta.cache).toBe(CacheOutcome.HIT);
    expect(result.value).toEqual({
      status: 200,
      body: 'ok',
      headers: {
        'X-Default': '1',
        'Content-Type': 'text/plain',
        'X-Value': '1',
        'Cache-Control': 'public, max-age=60',
      },
    });
  });

  it('keeps existing cache-control when no override is returned', async () => {
    const response: ResponseLike = {
      status: 200,
      headers: {
        'X-Value': '1',
      },
      body: 'ok',
    };
    const getOrSet = vi.fn(async (_key: string, fetcher: Fetcher<ResponseLike>, _options?: GetOrSetOptions<ResponseLike>) => {
      const value = await fetcher();
      return { value, meta: { cache: CacheOutcome.HIT } };
    });

    const result = await getOrSetResponse({
      getOrSet,
      request: { path: '/dogs' },
      keyFromRequest: (req) => req.path,
      fetcher: async () => response,
      cacheControl: () => undefined,
      defaultHeaders: {
        'Cache-Control': 'max-age=10',
      },
    });

    expect(result.value.headers['Cache-Control']).toBe('max-age=10');
    expect(result.value.headers['X-Value']).toBe('1');
  });
});
