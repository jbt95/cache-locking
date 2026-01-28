import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration, Effect } from 'effect';
import { PostgresCache, PostgresLeases } from '@adapters/postgres';
import { createPostgresTables, startPostgres, type PostgresTestContext } from '../support/postgres';
import { describeContainerIntegration, makeTestPrefix, wait } from './integration-helpers';

describeContainerIntegration('postgres adapter integration', () => {
  const prefix = makeTestPrefix('postgres');

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

  it('stores values and respects TTL', async () => {
    const cache = new PostgresCache<string>({
      client: postgres.pool,
      tableName: cacheTable,
      keyPrefix: `${prefix}cache:`,
    });

    await Effect.runPromise(cache.set('k', 'value', Duration.millis(200)));
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    await wait(250);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();
  }, 10000);

  it('acquires, marks ready, and releases leases', async () => {
    const leases = new PostgresLeases({
      client: postgres.pool,
      tableName: leasesTable,
      keyPrefix: `${prefix}lease:`,
    });

    const first = await Effect.runPromise(leases.acquire('k', 'owner-1', Duration.seconds(1)));
    expect(first.role).toBe('leader');

    const second = await Effect.runPromise(leases.acquire('k', 'owner-2', Duration.seconds(1)));
    expect(second.role).toBe('follower');

    await Effect.runPromise(leases.markReady('k'));
    const ready = await Effect.runPromise(leases.isReady('k'));
    expect(ready.ready).toBe(true);

    await Effect.runPromise(leases.release('k', 'owner-1'));
    const third = await Effect.runPromise(leases.acquire('k', 'owner-3', Duration.seconds(1)));
    expect(third.role).toBe('leader');
  }, 10000);
});
