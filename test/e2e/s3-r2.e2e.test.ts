import { afterAll, beforeAll, it } from 'vitest';
import { startMinio, type MinioTestContext } from '../support/minio';
import { describeContainerIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeContainerIntegration('s3 and r2 adapters e2e', () => {
  const s3Prefix = makeTestPrefix('s3-e2e');
  const r2Prefix = makeTestPrefix('r2-e2e');

  let minio: MinioTestContext;

  beforeAll(async () => {
    minio = await startMinio();
  });

  afterAll(async () => {
    if (minio) {
      await minio.stop();
    }
  });

  it('runs the full path with s3 adapter', async () => {
    const adapter = {
      type: 's3',
      options: {
        cache: { client: minio.client, bucket: minio.bucket, keyPrefix: `${s3Prefix}cache:` },
        leases: { client: minio.client, bucket: minio.bucket, keyPrefix: `${s3Prefix}lease:` },
      },
    } as const;

    await runFullPathE2E({ adapter });
  }, 10000);

  it('runs the full path with r2 adapter', async () => {
    const adapter = {
      type: 'r2',
      options: {
        cache: { client: minio.client, bucket: minio.bucket, keyPrefix: `${r2Prefix}cache:` },
        leases: { client: minio.client, bucket: minio.bucket, keyPrefix: `${r2Prefix}lease:` },
      },
    } as const;

    await runFullPathE2E({ adapter });
  }, 10000);
});
