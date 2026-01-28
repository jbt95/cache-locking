import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration, Effect } from 'effect';
import { R2Cache, R2Leases } from '@adapters/r2';
import { S3Cache, S3Leases } from '@adapters/s3';
import { startMinio, type MinioTestContext } from '../support/minio';
import { describeContainerIntegration, makeTestPrefix, wait } from './integration-helpers';

describeContainerIntegration('s3 and r2 adapter integration', () => {
  const prefix = makeTestPrefix('s3');

  let minio: MinioTestContext;

  beforeAll(async () => {
    minio = await startMinio();
  });

  afterAll(async () => {
    if (minio) {
      await minio.stop();
    }
  });

  it('stores values and respects TTL with s3 cache', async () => {
    const cache = new S3Cache<string>({
      client: minio.client,
      bucket: minio.bucket,
      keyPrefix: `${prefix}cache:`,
    });

    await Effect.runPromise(cache.set('k', 'value', Duration.millis(200)));
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    await wait(250);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();
  }, 10000);

  it('acquires, marks ready, and releases s3 leases', async () => {
    const leases = new S3Leases({
      client: minio.client,
      bucket: minio.bucket,
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

  it('stores values and respects TTL with r2 cache', async () => {
    const cache = new R2Cache<string>({
      client: minio.client,
      bucket: minio.bucket,
      keyPrefix: `${prefix}r2-cache:`,
    });

    await Effect.runPromise(cache.set('k', 'value', Duration.millis(200)));
    expect(await Effect.runPromise(cache.get('k'))).toBe('value');

    await wait(250);
    expect(await Effect.runPromise(cache.get('k'))).toBeNull();
  }, 10000);

  it('acquires, marks ready, and releases r2 leases', async () => {
    const leases = new R2Leases({
      client: minio.client,
      bucket: minio.bucket,
      keyPrefix: `${prefix}r2-lease:`,
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
