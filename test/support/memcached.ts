import * as memjs from 'memjs';
import type { StartedTestContainer } from 'testcontainers';
import { startContainer } from './testcontainers';

const memcachedPort = 11211;

export type MemcachedTestContext = {
  container: StartedTestContainer;
  client: memjs.Client<string | Buffer, string | Buffer | null>;
  stop: () => Promise<void>;
};

export const startMemcached = async (): Promise<MemcachedTestContext> => {
  const container = await startContainer('memcached:1.6', { port: memcachedPort });
  const server = `${container.getHost()}:${container.getMappedPort(memcachedPort)}`;
  const client = memjs.Client.create(server);

  const stop = async () => {
    client.quit();
    await container.stop();
  };

  return { container, client, stop };
};
