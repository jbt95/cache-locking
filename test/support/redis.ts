import { createClient } from 'redis';
import type { StartedTestContainer } from 'testcontainers';
import { startContainer } from './testcontainers';

const redisPort = 6379;

export type RedisTestContext = {
  container: StartedTestContainer;
  client: ReturnType<typeof createClient>;
  stop: () => Promise<void>;
};

export const startRedis = async (): Promise<RedisTestContext> => {
  const container = await startContainer('redis:7', { port: redisPort });
  const url = `redis://${container.getHost()}:${container.getMappedPort(redisPort)}`;
  const client = createClient({ url });
  await client.connect();

  const stop = async () => {
    await client.quit();
    await container.stop();
  };

  return { container, client, stop };
};
