import { Duration, Effect, Option } from 'effect';
import type { CoreClock, OwnerId, Sleep } from '@core/types';
import { fixedWaitStrategy } from '@core/wait-strategy';

/** Default lease TTL duration. */
export const DEFAULT_LEASE_TTL = Duration.seconds(15);
/** Default maximum wait duration. */
export const DEFAULT_WAIT_MAX = Duration.seconds(4);
/** Default wait step duration. */
export const DEFAULT_WAIT_STEP = Duration.millis(250);

/** Default clock using Date.now. */
export const defaultClock: CoreClock = {
  now: (): number => Date.now(),
};

/** Default sleep implementation using setTimeout. */
export const defaultSleep: Sleep = (duration) =>
  Effect.async<void>((resume) => {
    const delayMs = Math.max(0, Duration.toMillis(duration));
    const timeout = setTimeout(() => resume(Effect.succeed(undefined)), delayMs);
    return Effect.sync(() => clearTimeout(timeout));
  });

/** Default shouldCache predicate that always caches. */
export const defaultShouldCache = (): boolean => true;

/** Default wait strategy. */
export const defaultWaitStrategy = fixedWaitStrategy;

/** Create a random owner id. */
export const createOwnerId = (): OwnerId => `owner-${Math.random().toString(36).slice(2)}-${Date.now()}` as OwnerId;

/** Resolve a duration input with a fallback. */
export const resolveDuration = (
  value: Duration.DurationInput | undefined,
  fallback: Duration.Duration,
): Duration.Duration => {
  if (value === undefined) {
    return fallback;
  }
  return Option.getOrElse(Duration.decodeUnknown(value), () => fallback);
};

/** Resolve an optional duration input with a fallback. */
export const resolveOptionalDuration = (
  value: Duration.DurationInput | undefined,
  fallback?: Duration.Duration,
): Duration.Duration | undefined => {
  if (value === undefined) {
    return fallback;
  }
  return Option.getOrElse(Duration.decodeUnknown(value), () => fallback);
};
