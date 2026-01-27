# Cache Locking

Lease-based single-flight that prevents thundering herd on cache misses.

## The problem

When a hot key expires or misses, many concurrent requests can stampede the origin at once. This leads to:

- Bursty load spikes on your backend
- Redundant work (multiple callers recompute the same value)
- Higher tail latency for all callers

Cache locking solves this by coordinating a single "leader" to compute and fill the cache while others wait and re-check.

## About

This library coordinates cache fills with short-lived leases so only one caller does the expensive work while others wait and re-check the cache. The core is infra-agnostic and adapters supply the cache and lease backends, making it usable in Node, Workers, and serverless runtimes.

## How it works

1) Check cache for key.
2) On miss, try to acquire a lease.
3) The leader computes the value, caches it (if allowed), releases the lease, and marks ready.
4) Followers wait for readiness or until a timeout, then re-check the cache or fall back to compute themselves.

### Flow diagram

```text
Request A        Request B        Cache          Lease Store
   |                |               |                 |
   |---- get(k) ---->|               |                |
   |                |---- get(k) --->|                |
   |                |<--- miss ------|                |
   |<--- miss ------|               |                 |
   |---- acquire lease(k) --------------------------->|
   |<--- leader --------------------------------------|
   |---- compute ------------------------------------>|
   |---- set(k, v) ------------->|                    |
   |---- markReady(k) ------------------------------->|
   |---- release lease(k) --------------------------->|
   |                |---- wait/ready? --------------->|
   |                |<--- ready ----------------------|
   |                |---- get(k) --->|                |
   |                |<--- hit -------|                |
```

### Outcomes

- `MISS-LEADER`: leader computed and cached
- `MISS-LEADER-NOCACHE`: leader computed but skipped caching
- `MISS-FOLLOWER-HIT`: follower waited and got cache hit
- `MISS-FOLLOWER-FALLBACK`: follower timed out and computed

## Features

- Infra-agnostic core with small API surface
- Lease-based single-flight with bounded waiting
- Pluggable adapters (memory, Redis, Memcached, DynamoDB, MongoDB, Postgres, S3/R2, Cloudflare KV/D1)
- Optional readiness signaling to exit waits early
- Hooks, abort support, and pluggable wait strategies for better DX

## Installation

```bash
pnpm add cache-locking
```

### Requirements

- Node.js >= 18 (for tests and examples)

## Usage

```ts
import { Duration } from 'effect';
import { createAdapter, createCacheLocking } from 'cache-locking';

const adapter = createAdapter<string>({ type: 'memory' });
const { getOrSet, cache, leases } = await createCacheLocking({
  adapter,
  cacheTtl: Duration.seconds(60),
});

const { value, meta } = await getOrSet('user:42', async () => {
  return 'expensive-value';
});

console.log(value, meta.cache);
```

Time options accept Effect `Duration` inputs (for example `Duration.seconds(60)` or millisecond numbers).

### Effect API

If you're already using Effect, you can work with the Effect-based API and compose it with the rest of your program:

```ts
import { Effect } from 'effect';
import { createAdapter, createCacheLockingEffect } from 'cache-locking';

const program = Effect.gen(function* () {
  const adapter = createAdapter<string>({ type: 'memory' });
  const { getOrSet } = yield* createCacheLockingEffect({ adapter });
  return yield* getOrSet('user:42', async () => 'expensive-value');
});

const { value } = await Effect.runPromise(program);
```

### Effect Service API

If you prefer Effect services, you can inject a `CacheLockingService` into scope and use its accessors without passing options at the call site:

```ts
import { Effect } from 'effect';
import {
  CacheLockingService,
  createAdapter,
  createCacheLockingServiceLayer,
} from 'cache-locking';

const adapter = createAdapter<string>({ type: 'memory' });
const layer = createCacheLockingServiceLayer({ adapter });

const program = Effect.gen(function* () {
  return yield* CacheLockingService.getOrSet('user:42', async () => 'expensive-value');
}).pipe(Effect.provide(layer));

const { value } = await Effect.runPromise(program);
```

## Adapters

### Memory

- `MemoryCache` implements `Cache<V>`
- `MemoryLeases` implements `Leases`

### Redis

- `RedisCache` implements `Cache<V>` with string serialization
- `RedisLeases` implements `Leases` with ready keys
- `REDIS_RELEASE_SCRIPT` exported for manual scripting

```ts
import { createRedisCache, createRedisLeases } from 'cache-locking';
```

It uses `SET key value PX ttl NX` for acquire and a Lua script for compare-and-del release.

### Memcached (cache only)

- `MemcachedCache` implements `Cache<V>`
- `createCacheLocking` requires leases from another adapter or custom implementation

```ts
import { createMemcachedCache } from 'cache-locking';
```

### DynamoDB (cache only)

- `DynamoDbCache` implements `Cache<V>` with TTL attributes (default `ttl`)
- `createCacheLocking` requires leases from another adapter or custom implementation

```ts
import { createDynamoDbCache } from 'cache-locking';
```

### MongoDB

- `MongoCache` implements `Cache<V>`
- `MongoLeases` implements `Leases`
- Create separate collections for cache and leases; TTL index on `expiresAt` is recommended for cleanup

```ts
import { MongoCache, MongoLeases } from 'cache-locking';
```

### Postgres

- `PostgresCache` implements `Cache<V>`
- `PostgresLeases` implements `Leases`

Suggested tables:

```sql
CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE TABLE leases (key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, ready BOOLEAN NOT NULL DEFAULT false);
```

### S3 / R2

- `S3Cache` / `S3Leases` implement `Cache<V>` and `Leases`
- `R2Cache` / `R2Leases` are S3-compatible wrappers (configure the S3 client with the R2 endpoint)
- TTL is stored in object metadata (`expires_at`)
- Leases use conditional puts; expired leases are best-effort replaced

```ts
import { S3Cache, S3Leases } from 'cache-locking';
```

### Cloudflare KV / D1

- `CloudflareKvCache` implements `Cache<V>` (KV TTL is clamped to 60s minimum)
- `CloudflareD1Cache` + `CloudflareD1Leases` implement cache + leases on D1
- D1 defaults to `cache` / `leases` table names

Suggested D1 tables:

```sql
CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at INTEGER);
CREATE TABLE leases (key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at INTEGER NOT NULL, ready INTEGER NOT NULL DEFAULT 0);
```

Adapter configs:

- `cloudflare-kv`: `{ kv, leasesDb, cache?, leases? }`
- `cloudflare-d1`: `{ db, cache?, leases? }`

### Provider adapters

If you prefer a single adapter per provider, use the provider factories:

```ts
import { createAdapter, createCacheLocking } from 'cache-locking';

const adapter = createAdapter<string>({ type: 'redis', options: { client } });
const { getOrSet } = await createCacheLocking({ adapter });
```

Adapter types: `memory`, `redis`, `memcached`, `dynamodb`, `mongodb`, `postgres`, `s3`, `r2`, `cloudflare-kv`, `cloudflare-d1`.

## Portable HTTP Helper

`getOrSetResponse` wraps `getOrSet` for `ResponseLike` payloads:

```ts
import { getOrSetResponse } from 'cache-locking';
```

Define `ResponseLike` as `{ status, headers, body }` and let your cache adapter handle serialization.

## Design Guidance

- Cache adapters are responsible for serialization and TTL enforcement.
- Keep `leaseTtl` short; leases protect against stuck leaders.
- Favor `waitMax` smaller than average cache fill time to avoid piling up waits.
- Use `markReady`/`isReady` to exit follower waits early when leaders skip caching.

## Development

```bash
pnpm install
pnpm test
INTEGRATION_TESTS=1 pnpm test
MINIFLARE_TESTS=1 INTEGRATION_TESTS=1 pnpm test
pnpm build
```
