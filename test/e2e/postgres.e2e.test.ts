import { afterAll, beforeAll, it } from 'vitest';
import { createAdapter, createCacheLocking, type ResponseLike } from '@/index';
import { createPostgresTables, startPostgres, type PostgresTestContext } from '../support/postgres';
import { describeContainerIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeContainerIntegration('postgres adapter e2e', () => {
  const prefix = makeTestPrefix('postgres-e2e');

  let postgres: PostgresTestContext;
  let cacheTable: string;
  let leasesTable: string;

  beforeAll(async () => {
    postgres = await startPostgres();
    ({ cacheTable, leasesTable } = await createPostgresTables(postgres.pool));
  });

  afterAll(async () => {
    if (postgres) {
      await postgres.stop();
    }
  });

  it(
    'runs the full path',
    async () => {
      const adapter = createAdapter<ResponseLike>({
        type: 'postgres',
        options: {
          cache: { client: postgres.pool, tableName: cacheTable, keyPrefix: `${prefix}cache:` },
          leases: { client: postgres.pool, tableName: leasesTable, keyPrefix: `${prefix}lease:` },
        },
      });

      const locking = await createCacheLocking<ResponseLike>({ adapter });
      await runFullPathE2E({ locking });
    },
    10000,
  );
});
