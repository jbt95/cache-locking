import type { GetOrSetFn, GetOrSetOptions, GetOrSetResult } from '@core/types';

export type ResponseLike = {
  status: number;
  headers: Record<string, string>;
  body: Uint8Array | string;
};

export type GetOrSetResponseOptions<Req> = {
  getOrSet: GetOrSetFn<ResponseLike>;
  request: Req;
  keyFromRequest: (req: Req) => string;
  fetcher: (req: Req) => Promise<ResponseLike>;
  cacheKeyPrefix?: string;
  cacheControl?: string | ((response: ResponseLike) => string | undefined);
  defaultHeaders?: Record<string, string>;
} & GetOrSetOptions<ResponseLike>;

class ResponseComposer<Req> {
  buildKey(prefix: string | undefined, keyFromRequest: (req: Req) => string, request: Req): string {
    return (prefix ?? '') + keyFromRequest(request);
  }

  buildHeaders(
    value: ResponseLike,
    defaultHeaders: Record<string, string> | undefined,
    cacheControl: GetOrSetResponseOptions<Req>['cacheControl'],
  ): Record<string, string> {
    const headers = Object.assign({}, defaultHeaders ?? {});
    Object.assign(headers, value.headers);

    if (cacheControl) {
      const cacheControlValue = typeof cacheControl === 'function' ? cacheControl(value) : cacheControl;
      if (cacheControlValue) {
        headers['Cache-Control'] = cacheControlValue;
      }
    }

    return headers;
  }

  buildValue(value: ResponseLike, headers: Record<string, string>): ResponseLike {
    return {
      status: value.status,
      body: value.body,
      headers,
    };
  }
}

export const getOrSetResponse = async <Req>(
  options: GetOrSetResponseOptions<Req>,
): Promise<GetOrSetResult<ResponseLike>> => {
  const {
    getOrSet,
    request,
    keyFromRequest,
    fetcher,
    cacheKeyPrefix,
    cacheControl,
    defaultHeaders,
    cacheTtl,
    leaseTtl,
    waitMax,
    waitStep,
    shouldCache,
    ownerId,
    signal,
    waitStrategy,
    hooks,
  } = options;

  const composer = new ResponseComposer<Req>();
  const key = composer.buildKey(cacheKeyPrefix, keyFromRequest, request);

  const result = await getOrSet(key, () => fetcher(request), {
    cacheTtl,
    leaseTtl,
    waitMax,
    waitStep,
    shouldCache,
    ownerId,
    signal,
    waitStrategy,
    hooks,
  });

  const headers = composer.buildHeaders(result.value, defaultHeaders, cacheControl);

  return {
    value: composer.buildValue(result.value, headers),
    meta: result.meta,
  };
};
