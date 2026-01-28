import { describe, it } from 'vitest';
import { runFullPathE2E } from './e2e-helpers';

describe('memory adapter e2e', () => {
  it('runs the full path', async () => {
    const adapter = { type: 'memory' } as const;
    await runFullPathE2E({ adapter });
  });
});
