import { Miniflare } from 'miniflare';
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';

type MiniflareOptions = {
  kvName?: string;
  d1Name?: string;
};

export type MiniflareTestContext = {
  mf: Miniflare;
  kv: KVNamespace;
  db: D1Database;
  stop: () => Promise<void>;
};

export const startMiniflare = async (options?: MiniflareOptions): Promise<MiniflareTestContext> => {
  const kvName = options?.kvName ?? 'CACHE_KV';
  const d1Name = options?.d1Name ?? 'CACHE_DB';

  const mf = new Miniflare({
    modules: true,
    script: 'export default { fetch() { return new Response("ok"); } };',
    kvNamespaces: [kvName],
    d1Databases: [d1Name],
  });
  await mf.ready;

  const kv = (await mf.getKVNamespace(kvName)) as unknown as KVNamespace;
  const db = (await mf.getD1Database(d1Name)) as D1Database;
  await db.exec('CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at INTEGER)');
  await db.exec(
    'CREATE TABLE IF NOT EXISTS leases (key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at INTEGER NOT NULL, ready INTEGER NOT NULL DEFAULT 0)',
  );

  const stop = async () => {
    await mf.dispose();
  };

  return { mf, kv, db, stop };
};
