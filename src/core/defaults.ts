import { Duration, Option } from 'effect';
import type { CoreClock, OwnerId, Sleep } from '@core/types';
import { fixedWaitStrategy } from '@core/wait-strategy';

export const DEFAULT_LEASE_TTL = Duration.seconds(15);
export const DEFAULT_WAIT_MAX = Duration.seconds(4);
export const DEFAULT_WAIT_STEP = Duration.millis(250);

export const defaultClock: CoreClock = {
  now: (): number => Date.now(),
};

export const defaultSleep: Sleep = (duration): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, Duration.toMillis(duration)));

export const defaultShouldCache = (): boolean => true;

export const defaultWaitStrategy = fixedWaitStrategy;

export const createOwnerId = (): OwnerId => `owner-${Math.random().toString(36).slice(2)}-${Date.now()}` as OwnerId;

export const resolveDuration = (
  value: Duration.DurationInput | undefined,
  fallback: Duration.Duration,
): Duration.Duration => {
  if (value === undefined) {
    return fallback;
  }
  return Option.getOrElse(Duration.decodeUnknown(value), () => fallback);
};

export const resolveOptionalDuration = (
  value: Duration.DurationInput | undefined,
  fallback?: Duration.Duration,
): Duration.Duration | undefined => {
  if (value === undefined) {
    return fallback;
  }
  return Option.getOrElse(Duration.decodeUnknown(value), () => fallback);
};
