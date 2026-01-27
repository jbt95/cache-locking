import { afterAll, beforeAll, it } from 'vitest';
import { createAdapter, createCacheLocking, type ResponseLike } from '@/index';
import { createUniqueName } from '../support/ids';
import { startMongo, type MongoTestContext } from '../support/mongodb';
import { describeContainerIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeContainerIntegration('mongodb adapter e2e', () => {
  const prefix = makeTestPrefix('mongodb-e2e');

  let mongo: MongoTestContext;

  beforeAll(async () => {
    mongo = await startMongo();
  });

  afterAll(async () => {
    if (mongo) {
      await mongo.stop();
    }
  });

  it(
    'runs the full path',
    async () => {
      const db = mongo.client.db(createUniqueName('cache_locking_e2e', '_'));
      const cacheCollection = db.collection(`${prefix}cache`);
      const leasesCollection = db.collection(`${prefix}leases`);

      const adapter = createAdapter<ResponseLike>({
        type: 'mongodb',
        options: {
          cache: { collection: cacheCollection, keyPrefix: `${prefix}cache:` },
          leases: { collection: leasesCollection, keyPrefix: `${prefix}lease:` },
        },
      });

      const locking = await createCacheLocking<ResponseLike>({ adapter });
      await runFullPathE2E({ locking });
    },
    10000,
  );
});
