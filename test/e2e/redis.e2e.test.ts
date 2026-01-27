import { afterAll, beforeAll, it } from 'vitest';
import { createAdapter, createCacheLocking, type ResponseLike } from '@/index';
import { startRedis, type RedisTestContext } from '../support/redis';
import { describeContainerIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeContainerIntegration('redis adapter e2e', () => {
  const prefix = makeTestPrefix('redis-e2e');
  let redis: RedisTestContext;

  beforeAll(async () => {
    redis = await startRedis();
  });

  afterAll(async () => {
    if (redis) {
      await redis.stop();
    }
  });

  it(
    'runs the full path',
    async () => {
      const adapter = createAdapter<ResponseLike>({
        type: 'redis',
        options: {
          client: redis.client,
          cache: { keyPrefix: `${prefix}cache:` },
          leases: { keyPrefix: `${prefix}lease:` },
        },
      });

      const locking = await createCacheLocking<ResponseLike>({ adapter });
      await runFullPathE2E({ locking });
    },
    10000,
  );
});
