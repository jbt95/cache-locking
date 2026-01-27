import type { Duration } from 'effect';
import type { CacheLockingHooks } from '@core/types';

export class HookRunner<V> {
  private readonly base?: CacheLockingHooks<V>;
  private readonly override?: CacheLockingHooks<V>;

  constructor(base?: CacheLockingHooks<V>, override?: CacheLockingHooks<V>) {
    this.base = base;
    this.override = override;
  }

  async onHit(value: V, context: { key: string }): Promise<void> {
    if (this.base?.onHit) {
      await this.base.onHit(value, context);
    }
    if (this.override?.onHit) {
      await this.override.onHit(value, context);
    }
  }

  async onLeader(value: V, context: { key: string; leaseUntil: number; cached: boolean }): Promise<void> {
    if (this.base?.onLeader) {
      await this.base.onLeader(value, context);
    }
    if (this.override?.onLeader) {
      await this.override.onLeader(value, context);
    }
  }

  async onFollowerWait(context: {
    key: string;
    leaseUntil: number;
    waited: Duration.Duration;
    outcome: 'HIT' | 'FALLBACK';
  }): Promise<void> {
    if (this.base?.onFollowerWait) {
      await this.base.onFollowerWait(context);
    }
    if (this.override?.onFollowerWait) {
      await this.override.onFollowerWait(context);
    }
  }

  async onFallback(value: V, context: { key: string; leaseUntil: number; waited: Duration.Duration }): Promise<void> {
    if (this.base?.onFallback) {
      await this.base.onFallback(value, context);
    }
    if (this.override?.onFallback) {
      await this.override.onFallback(value, context);
    }
  }
}
