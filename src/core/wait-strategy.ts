import { Duration } from 'effect';
import type { WaitStrategy } from '@core/types';

export type BackoffWaitStrategyOptions = {
  initial?: Duration.DurationInput;
  max?: Duration.DurationInput;
  multiplier?: number;
  jitter?: number;
};

export const fixedWaitStrategy: WaitStrategy = ({ waitStep }) => waitStep;

export const createBackoffWaitStrategy = (options: BackoffWaitStrategyOptions = {}): WaitStrategy => {
  const multiplier = options.multiplier ?? 2;
  const jitter = options.jitter ?? 0.2;
  const maxMs = Duration.toMillis(options.max ?? Duration.seconds(5));

  return ({ attempt, waitStep }) => {
    const baseMs = Duration.toMillis(options.initial ?? waitStep);
    const exponential = baseMs * Math.pow(multiplier, Math.max(0, attempt));
    const capped = Math.min(exponential, maxMs);
    const randomJitter = capped * jitter * (Math.random() - 0.5) * 2;
    return Duration.millis(Math.max(0, capped + randomJitter));
  };
};
