import { Pool } from 'pg';
import type { StartedTestContainer } from 'testcontainers';
import { createUniqueName } from './ids';
import { startContainer } from './testcontainers';

const postgresPort = 5432;

type PostgresConfig = {
  user: string;
  password: string;
  database: string;
};

const defaultConfig: PostgresConfig = {
  user: 'postgres',
  password: 'postgres',
  database: 'cache_locking',
};

export type PostgresTestContext = {
  container: StartedTestContainer;
  pool: Pool;
  config: PostgresConfig;
  stop: () => Promise<void>;
};

export const startPostgres = async (config?: Partial<PostgresConfig>): Promise<PostgresTestContext> => {
  const resolved = { ...defaultConfig, ...config };
  const container = await startContainer('postgres:15-alpine', {
    port: postgresPort,
    env: {
      POSTGRES_USER: resolved.user,
      POSTGRES_PASSWORD: resolved.password,
      POSTGRES_DB: resolved.database,
    },
  });

  const host = container.getHost();
  const port = container.getMappedPort(postgresPort);
  const pool = new Pool({
    host,
    port,
    user: resolved.user,
    password: resolved.password,
    database: resolved.database,
  });

  const stop = async () => {
    await pool.end();
    await container.stop();
  };

  return { container, pool, config: resolved, stop };
};

export const createPostgresTables = async (
  pool: Pool,
  options?: { cacheTable?: string; leasesTable?: string },
): Promise<{ cacheTable: string; leasesTable: string }> => {
  const cacheTable = options?.cacheTable ?? createUniqueName('cache', '_');
  const leasesTable = options?.leasesTable ?? createUniqueName('leases', '_');

  await pool.query(`CREATE TABLE ${cacheTable} (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ)`);
  await pool.query(
    `CREATE TABLE ${leasesTable} (key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, ready BOOLEAN NOT NULL DEFAULT false)`,
  );

  return { cacheTable, leasesTable };
};
