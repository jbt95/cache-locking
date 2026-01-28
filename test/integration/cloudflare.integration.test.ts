import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration, Effect } from 'effect';
import { CloudflareD1Cache, CloudflareD1Leases, CloudflareKvCache } from '@adapters/cloudflare';
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import { startMiniflare, type MiniflareTestContext } from '../support/miniflare';
import { describeMiniflareIntegration, makeTestPrefix, wait } from './integration-helpers';

describeMiniflareIntegration('cloudflare adapters integration', () => {
  const prefix = makeTestPrefix('cloudflare');

  let mf: MiniflareTestContext;
  let kv: KVNamespace;
  let db: D1Database;

  beforeAll(async () => {
    mf = await startMiniflare();
    kv = mf.kv;
    db = mf.db;
  });

  afterAll(async () => {
    if (mf) {
      await mf.stop();
    }
  });

  it('stores values and respects TTL with KV cache + D1 leases', async () => {
    const cache = new CloudflareKvCache<string>({ kv, keyPrefix: `${prefix}kv:` });
    const leases = new CloudflareD1Leases({ db, keyPrefix: `${prefix}lease:` });

    await Effect.runPromise(cache.set('k', 'value', Duration.seconds(60)));
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    const first = await Effect.runPromise(leases.acquire('k', 'owner-1', Duration.seconds(1)));
    expect(first.role).toBe('leader');

    await Effect.runPromise(leases.markReady('k'));
    const ready = await Effect.runPromise(leases.isReady('k'));
    expect(ready.ready).toBe(true);
  }, 10000);

  it('stores values and respects TTL with D1 cache + D1 leases', async () => {
    const cache = new CloudflareD1Cache<string>({ db, keyPrefix: `${prefix}d1:` });
    const leases = new CloudflareD1Leases({ db, keyPrefix: `${prefix}lease:` });

    await Effect.runPromise(cache.set('k', 'value', Duration.millis(200)));
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    await wait(250);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();

    const first = await Effect.runPromise(leases.acquire('k', 'owner-1', Duration.seconds(1)));
    expect(first.role).toBe('leader');

    const second = await Effect.runPromise(leases.acquire('k', 'owner-2', Duration.seconds(1)));
    expect(second.role).toBe('follower');
  }, 10000);
});
