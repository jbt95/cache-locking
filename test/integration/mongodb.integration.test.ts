import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration, Effect } from 'effect';
import { MongoCache, MongoLeases } from '@adapters/mongodb';
import { createUniqueName } from '../support/ids';
import { startMongo, type MongoTestContext } from '../support/mongodb';
import { describeContainerIntegration, makeTestPrefix, wait } from './integration-helpers';

describeContainerIntegration('mongodb adapter integration', () => {
  const prefix = makeTestPrefix('mongodb');

  let mongo: MongoTestContext;

  beforeAll(async () => {
    mongo = await startMongo();
  });

  afterAll(async () => {
    if (mongo) {
      await mongo.stop();
    }
  });

  it('stores values and respects TTL', async () => {
    const db = mongo.client.db(createUniqueName('cache_locking', '_'));
    const collection = db.collection(`${prefix}cache`);
    const cache = new MongoCache<string>({ collection, keyPrefix: `${prefix}cache:` });

    await Effect.runPromise(cache.set('k', 'value', Duration.millis(200)));
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    await wait(250);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();
  }, 10000);

  it('acquires, marks ready, and releases leases', async () => {
    const db = mongo.client.db(createUniqueName('cache_locking', '_'));
    const collection = db.collection(`${prefix}leases`);
    const leases = new MongoLeases({ collection, keyPrefix: `${prefix}lease:` });

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
