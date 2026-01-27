import { describe, expect, it, vi } from 'vitest';
import { Duration } from 'effect';
import { createBackoffWaitStrategy, fixedWaitStrategy } from '@core/wait-strategy';

describe('wait strategies', () => {
  it('returns the waitStep for the fixed strategy', () => {
    const waitStep = Duration.millis(123);
    const delay = fixedWaitStrategy({
      attempt: 0,
      elapsed: Duration.millis(0),
      remaining: Duration.seconds(1),
      waitMax: Duration.seconds(1),
      waitStep,
    });

    expect(Duration.toMillis(delay)).toBe(123);
  });

  it('caps backoff delays at the configured max', () => {
    const strategy = createBackoffWaitStrategy({
      initial: Duration.millis(10),
      max: Duration.millis(25),
      multiplier: 3,
      jitter: 0,
    });

    const delay = strategy({
      attempt: 3,
      elapsed: Duration.millis(0),
      remaining: Duration.seconds(1),
      waitMax: Duration.seconds(1),
      waitStep: Duration.millis(10),
    });

    expect(Duration.toMillis(delay)).toBe(25);
  });

  it('applies jitter and clamps negative delays to zero', () => {
    const spy = vi.spyOn(Math, 'random').mockReturnValue(0);
    const strategy = createBackoffWaitStrategy({
      initial: Duration.millis(10),
      max: Duration.millis(100),
      multiplier: 1,
      jitter: 1,
    });

    const delay = strategy({
      attempt: 0,
      elapsed: Duration.millis(0),
      remaining: Duration.seconds(1),
      waitMax: Duration.seconds(1),
      waitStep: Duration.millis(10),
    });

    expect(Duration.toMillis(delay)).toBe(0);
    spy.mockRestore();
  });
});
