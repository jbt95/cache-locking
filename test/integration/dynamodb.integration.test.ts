import { afterAll, beforeAll, expect, it } from 'vitest';
import { Duration } from 'effect';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { DynamoDbCache } from '@/index';
import { startDynamoDb, type DynamoDbTestContext } from '../support/dynamodb';
import { describeContainerIntegration, makeTestPrefix } from './integration-helpers';

describeContainerIntegration('dynamodb adapter integration', () => {
  const prefix = makeTestPrefix('dynamodb');

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
    'stores values and writes TTL attributes',
    async () => {
      const cache = new DynamoDbCache<string>({
        client: dynamo.docClient,
        tableName: dynamo.tableName,
        keyAttribute: dynamo.keyAttribute,
        ttlAttribute: dynamo.ttlAttribute,
        keyPrefix: prefix,
      });

      await cache.set('k', 'value', Duration.seconds(1));
      expect(await cache.get('k')).toBe('value');

      const response = await dynamo.docClient.send(
        new GetCommand({
          TableName: dynamo.tableName,
          Key: { [dynamo.keyAttribute]: `${prefix}k` },
        }),
      );

      const item = response.Item as Record<string, unknown> | undefined;
      expect(item?.[dynamo.keyAttribute]).toBe(`${prefix}k`);
      expect(typeof item?.[dynamo.ttlAttribute]).toBe('number');

      const ttlSeconds = item?.[dynamo.ttlAttribute] as number | undefined;
      const nowSeconds = Math.floor(Date.now() / 1000);
      if (ttlSeconds !== undefined) {
        expect(ttlSeconds).toBeGreaterThanOrEqual(nowSeconds);
      }
    },
    10000,
  );
});
