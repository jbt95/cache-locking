import { afterAll, beforeAll, it } from 'vitest';
import { MemoryLeases, createAdapter, createCacheLocking, type ResponseLike } from '@/index';
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

  it(
    'runs the full path with memory leases',
    async () => {
      const adapter = createAdapter<ResponseLike>({
        type: 'dynamodb',
        options: {
          client: dynamo.docClient,
          tableName: dynamo.tableName,
          keyAttribute: dynamo.keyAttribute,
          ttlAttribute: dynamo.ttlAttribute,
          keyPrefix: prefix,
        },
      });

      const locking = await createCacheLocking<ResponseLike>({
        adapter,
        leases: new MemoryLeases(),
      });

      await runFullPathE2E({ locking });
    },
    10000,
  );
});
