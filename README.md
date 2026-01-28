# Cache Locking

Lease-based single-flight that prevents thundering herd on cache misses.

## About

This library coordinates cache fills with short-lived leases so only one caller does the expensive work while others wait and re-check. The public API is a single Effect entrypoint: `Cache.getOrSet`.

## Features

- Single-call `Cache.getOrSet` API with Effect-only fetchers and hooks
- Lease-based single-flight with bounded waiting
- Pluggable adapters (memory, Redis, Memcached, DynamoDB, MongoDB, Postgres, S3/R2, Cloudflare KV/D1)
- Abort support, typed errors, and customizable wait strategies

## Installation

```bash
pnpm add cache-locking
```

### Requirements

- Node.js >= 18

## Usage

### Cache.getOrSet

```ts
import { Effect } from 'effect';
import { Cache } from 'cache-locking';

const program = Cache.getOrSet({
  adapter: 'memory',
  key: 'user:42',
  fetcher: () =>
    Effect.gen(function* () {
      yield* Effect.log('fetching user');
      return 'expensive-value';
    }),
  hooks: {
    onLeader: (_value, context) => Effect.log('cache fill', { key: context.key, cached: context.cached }),
  },
});

const { value, meta } = await Effect.runPromise(program);

console.log(value, meta.cache);
```

Time options accept Effect `Duration` inputs (for example `Duration.seconds(60)` or millisecond numbers).
Fetchers and hooks must return `Effect` values.

### Adapter configuration

Use adapter config objects for backends that need options:

```ts
import { Effect } from 'effect';
import { Cache, type AdapterConfig } from 'cache-locking';

const adapter: AdapterConfig = {
  type: 'redis',
  options: {
    client,
    cache: { keyPrefix: 'cache:' },
    leases: { keyPrefix: 'lease:' },
  },
} as const;

const program = Cache.getOrSet({
  adapter,
  key: 'user:42',
  fetcher: () => Effect.succeed('value'),
});
```

If an adapter only provides a cache (for example Memcached or DynamoDB), supply a `leases` implementation in the options.

## Errors

`Cache.getOrSet` fails with `CacheLockingError` or your fetcher/hook errors.

```ts
import { Effect } from 'effect';
import { formatCacheLockingError, matchCacheLockingError } from 'cache-locking';

const handled = program.pipe(
  Effect.catchAll((error) =>
    Effect.sync(() =>
      matchCacheLockingError(error, {
        CACHE_GET_FAILED: (err) => `cache read failed for ${err.context.key}`,
        _: (err) => formatCacheLockingError(err),
      }),
    ),
  ),
);
```

## Adapters

Supported adapter types:

- `memory`
- `redis`
- `memcached`
- `dynamodb`
- `mongodb`
- `postgres`
- `cloudflare-kv`
- `cloudflare-d1`
- `s3`
- `r2`
