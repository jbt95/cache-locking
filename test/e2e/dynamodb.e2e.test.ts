import { afterAll, beforeAll, it } from 'vitest';
import { MemoryLeases } from '@adapters/memory';
import { startDynamoDb, type DynamoDbTestContext } from '../support/dynamodb';
import { describeContainerIntegration, makeTestPrefix } from '../integration/integration-helpers';
import { runFullPathE2E } from './e2e-helpers';

describeContainerIntegration('dynamodb adapter e2e', () => {
  const prefix = makeTestPrefix('dynamodb-e2e');

  let dynamo: DynamoDbTestContext;

  beforeAll(async () => {
    dynamo = await startDynamoDb();
  });

  afterAll(async () => {
    if (dynamo) {
      await dynamo.stop();
    }
  });

  it('runs the full path with memory leases', async () => {
    const adapter = {
      type: 'dynamodb',
      options: {
        client: dynamo.docClient,
        tableName: dynamo.tableName,
        keyAttribute: dynamo.keyAttribute,
        ttlAttribute: dynamo.ttlAttribute,
        keyPrefix: prefix,
      },
    } as const;

    await runFullPathE2E({ adapter, leases: new MemoryLeases() });
  }, 10000);
});
