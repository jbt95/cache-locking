import { describe, it } from 'vitest';
import { createAdapter, createCacheLocking, type ResponseLike } from '@/index';
import { runFullPathE2E } from './e2e-helpers';

describe('memory adapter e2e', () => {
  it('runs the full path', async () => {
    const adapter = createAdapter<ResponseLike>({ type: 'memory' });
    const locking = await createCacheLocking<ResponseLike>({ adapter });

    await runFullPathE2E({ locking });
  });
});
