import { MongoClient } from 'mongodb';
import type { StartedTestContainer } from 'testcontainers';
import { startContainer } from './testcontainers';

const mongoPort = 27017;

export type MongoTestContext = {
  container: StartedTestContainer;
  client: MongoClient;
  stop: () => Promise<void>;
};

export const startMongo = async (): Promise<MongoTestContext> => {
  const container = await startContainer('mongo:7', { port: mongoPort });
  const url = `mongodb://${container.getHost()}:${container.getMappedPort(mongoPort)}`;
  const client = new MongoClient(url);
  await client.connect();

  const stop = async () => {
    await client.close();
    await container.stop();
  };

  return { container, client, stop };
};
