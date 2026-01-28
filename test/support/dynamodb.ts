import { CreateTableCommand, DeleteTableCommand, DescribeTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { StartedTestContainer } from 'testcontainers';
import { createUniqueName } from './ids';
import { startContainer } from './testcontainers';

const dynamoPort = 8000;

type DynamoDbTableOptions = {
  tableName?: string;
  keyAttribute?: string;
  ttlAttribute?: string;
};

export type DynamoDbTestContext = {
  container: StartedTestContainer;
  client: DynamoDBClient;
  docClient: DynamoDBDocumentClient;
  tableName: string;
  keyAttribute: string;
  ttlAttribute: string;
  stop: () => Promise<void>;
};

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const waitForTableActive = async (client: DynamoDBClient, tableName: string): Promise<void> => {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    try {
      const response = await client.send(new DescribeTableCommand({ TableName: tableName }));
      if (response.Table?.TableStatus === 'ACTIVE') {
        return;
      }
    } catch {
      // Table not ready yet.
    }
    await wait(200);
  }
  throw new Error(`DynamoDB table ${tableName} not ready`);
};

export const startDynamoDb = async (options?: DynamoDbTableOptions): Promise<DynamoDbTestContext> => {
  const container = await startContainer('amazon/dynamodb-local:latest', {
    port: dynamoPort,
    command: ['-jar', 'DynamoDBLocal.jar', '-sharedDb', '-inMemory'],
  });

  const endpoint = `http://${container.getHost()}:${container.getMappedPort(dynamoPort)}`;
  const client = new DynamoDBClient({
    region: 'local',
    endpoint,
    credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
  });
  const docClient = DynamoDBDocumentClient.from(client);

  const keyAttribute = options?.keyAttribute ?? 'key';
  const ttlAttribute = options?.ttlAttribute ?? 'ttl';
  const tableName = options?.tableName ?? createUniqueName('cache-locking', '-');

  await client.send(
    new CreateTableCommand({
      TableName: tableName,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: keyAttribute, AttributeType: 'S' }],
      KeySchema: [{ AttributeName: keyAttribute, KeyType: 'HASH' }],
    }),
  );
  await waitForTableActive(client, tableName);

  const stop = async () => {
    await client.send(new DeleteTableCommand({ TableName: tableName }));
    client.destroy();
    await container.stop();
  };

  return { container, client, docClient, tableName, keyAttribute, ttlAttribute, stop };
};
