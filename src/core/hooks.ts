import { Effect } from 'effect';
import type { Duration } from 'effect';
import type { CacheLockingError } from '@core/errors';
import { HookFailed } from '@core/errors';
import { Phase } from '@core/phases';
import type { CacheLockingHookResult, CacheLockingHooks, Key } from '@core/types';
import { PhaseRunner } from '@core/phase-runner';

/** Runs base and override hooks with cache locking error mapping. */
export class HookRunner<V, EBase = never, RBase = never, EOverride = never, ROverride = never> {
  private readonly base?: CacheLockingHooks<V, EBase, RBase>;
  private readonly override?: CacheLockingHooks<V, EOverride, ROverride>;
  private readonly phaseRunner: PhaseRunner;
  private readonly key: Key;

  constructor(
    phaseRunner: PhaseRunner,
    key: Key,
    base?: CacheLockingHooks<V, EBase, RBase>,
    override?: CacheLockingHooks<V, EOverride, ROverride>,
  ) {
    this.phaseRunner = phaseRunner;
    this.key = key;
    this.base = base;
    this.override = override;
  }

  onHit(
    value: V,
    context: { key: string },
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    const baseHook = this.base?.onHit;
    const overrideHook = this.override?.onHit;
    return this.runHooks(
      Phase.HooksOnHit,
      `hooks.onHit failed for key "${context.key}"`,
      baseHook ? () => baseHook(value, context) : undefined,
      overrideHook ? () => overrideHook(value, context) : undefined,
    );
  }

  onLeader(
    value: V,
    context: { key: string; leaseUntil: number; cached: boolean },
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    const baseHook = this.base?.onLeader;
    const overrideHook = this.override?.onLeader;
    return this.runHooks(
      Phase.HooksOnLeader,
      `hooks.onLeader failed for key "${context.key}"`,
      baseHook ? () => baseHook(value, context) : undefined,
      overrideHook ? () => overrideHook(value, context) : undefined,
    );
  }

  onFollowerWait(context: {
    key: string;
    leaseUntil: number;
    waited: Duration.Duration;
    outcome: 'HIT' | 'FALLBACK';
  }): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    const baseHook = this.base?.onFollowerWait;
    const overrideHook = this.override?.onFollowerWait;
    return this.runHooks(
      Phase.HooksOnFollowerWait,
      `hooks.onFollowerWait failed for key "${context.key}"`,
      baseHook ? () => baseHook(context) : undefined,
      overrideHook ? () => overrideHook(context) : undefined,
    );
  }

  onFallback(
    value: V,
    context: { key: string; leaseUntil: number; waited: Duration.Duration },
  ): Effect.Effect<void, CacheLockingError | EBase | EOverride, RBase | ROverride> {
    const baseHook = this.base?.onFallback;
    const overrideHook = this.override?.onFallback;
    return this.runHooks(
      Phase.HooksOnFallback,
      `hooks.onFallback failed for key "${context.key}"`,
      baseHook ? () => baseHook(value, context) : undefined,
      overrideHook ? () => overrideHook(value, context) : undefined,
    );
  }

  private runHooks<E1, R1, E2, R2>(
    phase: Phase,
    message: string,
    baseHook: (() => CacheLockingHookResult<E1, R1>) | undefined,
    overrideHook: (() => CacheLockingHookResult<E2, R2>) | undefined,
  ): Effect.Effect<void, CacheLockingError | E1 | E2, R1 | R2> {
    return this.runHook(phase, message, baseHook).pipe(
      Effect.flatMap(() => this.runHook(phase, message, overrideHook)),
    );
  }

  private runHook<E, R>(
    phase: Phase,
    message: string,
    hook: (() => CacheLockingHookResult<E, R>) | undefined,
  ): Effect.Effect<void, CacheLockingError | E, R> {
    if (!hook) {
      return Effect.succeed(undefined);
    }

    const errorContext = { key: this.key, phase, adapter: 'hooks' } as const;

    return Effect.try({
      try: () => hook(),
      catch: (cause) => new HookFailed(message, errorContext, cause),
    }).pipe(
      Effect.flatMap((result) => {
        if (!Effect.isEffect(result)) {
          return Effect.fail(new HookFailed(`${message}; hooks must return Effect values`, errorContext, result));
        }
        return this.phaseRunner.runEffect(phase, { key: this.key }, message, result);
      }),
    );
  }
}
