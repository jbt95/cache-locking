import { describe, expect, it } from 'vitest';
import { Duration, Effect } from 'effect';
import { HookRunner } from '@core/hooks';
import { PhaseRunner } from '@core/phase-runner';
import type { Key } from '@core/types';

describe('HookRunner', () => {
  it('executes base hooks before override hooks', async () => {
    const calls: string[] = [];
    const base = {
      onHit: () => Effect.sync(() => calls.push('base-hit')),
      onLeader: () => Effect.sync(() => calls.push('base-leader')),
      onFollowerWait: () => Effect.sync(() => calls.push('base-wait')),
      onFallback: () => Effect.sync(() => calls.push('base-fallback')),
    };
    const override = {
      onHit: () => Effect.sync(() => calls.push('override-hit')),
      onLeader: () => Effect.sync(() => calls.push('override-leader')),
      onFollowerWait: () => Effect.sync(() => calls.push('override-wait')),
      onFallback: () => Effect.sync(() => calls.push('override-fallback')),
    };
    const hooks = new HookRunner(new PhaseRunner(), 'k' as Key, base, override);

    await Effect.runPromise(hooks.onHit('value', { key: 'k' }));
    await Effect.runPromise(hooks.onLeader('value', { key: 'k', leaseUntil: 1, cached: true }));
    await Effect.runPromise(
      hooks.onFollowerWait({
        key: 'k',
        leaseUntil: 1,
        waited: Duration.millis(5),
        outcome: 'HIT',
      }),
    );
    await Effect.runPromise(hooks.onFallback('value', { key: 'k', leaseUntil: 1, waited: Duration.millis(5) }));

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
