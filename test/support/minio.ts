import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3';
import type { StartedTestContainer } from 'testcontainers';
import { createUniqueName } from './ids';
import { startContainer } from './testcontainers';

const minioPort = 9000;

type MinioOptions = {
  accessKeyId?: string;
  secretAccessKey?: string;
  bucket?: string;
};

export type MinioTestContext = {
  container: StartedTestContainer;
  client: S3Client;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  stop: () => Promise<void>;
};

export const startMinio = async (options?: MinioOptions): Promise<MinioTestContext> => {
  const accessKeyId = options?.accessKeyId ?? 'minio';
  const secretAccessKey = options?.secretAccessKey ?? 'minio123';
  const container = await startContainer('minio/minio', {
    port: minioPort,
    env: {
      MINIO_ROOT_USER: accessKeyId,
      MINIO_ROOT_PASSWORD: secretAccessKey,
    },
    command: ['server', '/data'],
  });

  const endpoint = `http://${container.getHost()}:${container.getMappedPort(minioPort)}`;
  const client = new S3Client({
    endpoint,
    forcePathStyle: true,
    region: 'us-east-1',
    credentials: { accessKeyId, secretAccessKey },
  });

  const bucket = options?.bucket ?? createUniqueName('cache-locking', '-');
  await client.send(new CreateBucketCommand({ Bucket: bucket }));

  const stop = async () => {
    client?.destroy?.();
    await container.stop();
  };

  return { container, client, bucket, accessKeyId, secretAccessKey, stop };
};
