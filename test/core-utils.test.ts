import { describe, expect, it } from 'vitest';
import { Cause, Duration, Effect, Either, Exit } from 'effect';
import { CacheGetFailed, isCacheLockingError } from '@core/errors';
import { PhaseRunner } from '@core/phase-runner';
import { Phase } from '@core/phases';
import { resolveDuration, resolveOptionalDuration } from '@core/defaults';

describe('errors', () => {
  it('detects cache locking errors by tag', () => {
    const error = new CacheGetFailed('boom', { key: 'k', phase: Phase.CacheGet, adapter: 'cache' });

    expect(isCacheLockingError(error)).toBe(true);
    expect(isCacheLockingError({ _tag: 'UNKNOWN' })).toBe(false);
    expect(isCacheLockingError(null)).toBe(false);
  });
});

describe('PhaseRunner', () => {
  it('wraps unknown errors with phase-specific errors', async () => {
    const runner = new PhaseRunner();
    const effect = runner.runSync(Phase.CacheGet, { key: 'k' }, 'cache.get failed for key "k"', () => {
      throw new Error('boom');
    });

    const exit = await Effect.runPromiseExit(effect);

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const failure = Cause.failureOrCause(exit.cause);
      expect(Either.isLeft(failure)).toBe(true);
      if (Either.isLeft(failure)) {
        expect(failure.left).toMatchObject({
          _tag: 'CACHE_GET_FAILED',
          context: { key: 'k', phase: Phase.CacheGet, adapter: 'cache' },
        });
      }
    }
  });

  it('passes through existing cache locking errors', async () => {
    const runner = new PhaseRunner();
    const existing = new CacheGetFailed('boom', { key: 'k', phase: Phase.CacheGet, adapter: 'cache' });
    const effect = runner.runSync(Phase.CacheGet, { key: 'k' }, 'ignored', () => {
      throw existing;
    });

    const exit = await Effect.runPromiseExit(effect);

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const failure = Cause.failureOrCause(exit.cause);
      expect(Either.isLeft(failure)).toBe(true);
      if (Either.isLeft(failure)) {
        expect(failure.left).toMatchObject({
          _tag: 'CACHE_GET_FAILED',
          message: 'boom',
          context: { key: 'k', phase: Phase.CacheGet, adapter: 'cache' },
        });
      }
    }
  });
});

describe('defaults', () => {
  it('resolves fallback durations when input is missing or invalid', () => {
    const fallback = Duration.seconds(2);

    expect(Duration.toMillis(resolveDuration(undefined, fallback))).toBe(Duration.toMillis(fallback));
    expect(Duration.toMillis(resolveDuration('nope' as unknown as Duration.DurationInput, fallback))).toBe(
      Duration.toMillis(fallback),
    );
  });

  it('resolves optional durations with or without fallbacks', () => {
    const fallback = Duration.millis(250);
    const resolved = resolveOptionalDuration(undefined, fallback);

    expect(resolved).toBeDefined();
    expect(Duration.toMillis(resolved!)).toBe(Duration.toMillis(fallback));
    expect(resolveOptionalDuration(undefined, undefined)).toBeUndefined();
  });
});
