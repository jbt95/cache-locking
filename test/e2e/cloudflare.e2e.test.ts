import { afterAll, beforeAll, it } from 'vitest';
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import { startMiniflare, type MiniflareTestContext } from '../support/miniflare';
import { describeMiniflareIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeMiniflareIntegration('cloudflare adapters e2e', () => {
  const kvPrefix = makeTestPrefix('cloudflare-kv-e2e');
  const d1Prefix = makeTestPrefix('cloudflare-d1-e2e');

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

  it('runs the full path with KV cache + D1 leases', async () => {
    const adapter = {
      type: 'cloudflare-kv',
      options: {
        kv,
        leasesDb: db,
        cache: { keyPrefix: `${kvPrefix}cache:` },
        leases: { keyPrefix: `${kvPrefix}lease:` },
      },
    } as const;

    await runFullPathE2E({ adapter });
  }, 10000);

  it('runs the full path with D1 cache + D1 leases', async () => {
    const adapter = {
      type: 'cloudflare-d1',
      options: {
        db,
        cache: { keyPrefix: `${d1Prefix}cache:` },
        leases: { keyPrefix: `${d1Prefix}lease:` },
      },
    } as const;

    await runFullPathE2E({ adapter });
  }, 10000);
});
