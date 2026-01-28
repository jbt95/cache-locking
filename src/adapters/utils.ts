import { Duration } from 'effect';

/** Convert a duration input into non-negative milliseconds. */
export const toMillisClamped = (duration: Duration.DurationInput): number => Math.max(0, Duration.toMillis(duration));

/** Parse epoch millis from Date, number, or string values. */
export const readEpochMillis = (value: unknown): number | null => {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      return asNumber;
    }
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
};

/** Quote an identifier for SQL usage. */
export const quoteIdentifier = (value: string): string => `"${value.replace(/"/g, '""')}"`;
