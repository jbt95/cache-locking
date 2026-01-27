import { describe, expect, it } from 'vitest';
import { Duration } from 'effect';
import { HookRunner } from '@core/hooks';

describe('HookRunner', () => {
  it('executes base hooks before override hooks', async () => {
    const calls: string[] = [];
    const base = {
      onHit: async () => {
        calls.push('base-hit');
      },
      onLeader: async () => {
        calls.push('base-leader');
      },
      onFollowerWait: async () => {
        calls.push('base-wait');
      },
      onFallback: async () => {
        calls.push('base-fallback');
      },
    };
    const override = {
      onHit: async () => {
        calls.push('override-hit');
      },
      onLeader: async () => {
        calls.push('override-leader');
      },
      onFollowerWait: async () => {
        calls.push('override-wait');
      },
      onFallback: async () => {
        calls.push('override-fallback');
      },
    };
    const hooks = new HookRunner(base, override);

    await hooks.onHit('value', { key: 'k' });
    await hooks.onLeader('value', { key: 'k', leaseUntil: 1, cached: true });
    await hooks.onFollowerWait({
      key: 'k',
      leaseUntil: 1,
      waited: Duration.millis(5),
      outcome: 'HIT',
    });
    await hooks.onFallback('value', { key: 'k', leaseUntil: 1, waited: Duration.millis(5) });

    expect(calls).toEqual([
      'base-hit',
      'override-hit',
      'base-leader',
      'override-leader',
      'base-wait',
      'override-wait',
      'base-fallback',
      'override-fallback',
    ]);
  });
});
