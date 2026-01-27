import { Duration, Effect, Either, ParseResult, Schema } from 'effect';
import { ValidationError } from '@core/errors';
import type { Cache, CacheLockingErrorContext, Key, Leases, OwnerId } from '@core/types';

type ValidationIssue = {
  readonly path: ReadonlyArray<PropertyKey>;
  readonly message: string;
};

const toIssue = (path: ReadonlyArray<PropertyKey>, message: string): ValidationIssue => ({ path, message });
const message =
  (text: string): (() => string) =>
  () =>
    text;

const functionSchema = (label: string): Schema.Schema<(...args: unknown[]) => unknown, unknown, never> =>
  Schema.Unknown.pipe(
    Schema.filter((value): value is (...args: unknown[]) => unknown => typeof value === 'function', {
      message: message(`${label} must be a function`),
    }),
  );

const predicateSchema = (label: string): Schema.Schema<(...args: unknown[]) => unknown, unknown, never> =>
  functionSchema(label);

const nonEmptyStringSchema = (label: string): Schema.Schema<string, string, never> =>
  Schema.String.pipe(
    Schema.filter((value) => value.trim().length > 0, { message: message(`${label} must be a non-empty string`) }),
  );

const durationSchema = (label: string): Schema.Schema<Duration.Duration, number | Duration.Duration, never> =>
  Schema.Union(Schema.DurationFromMillis, Schema.DurationFromSelf).pipe(
    Schema.filter(Duration.isFinite, { message: message(`${label} must be a finite duration`) }),
  );

const objectWithFunctionsSchema = (
  label: string,
  required: string[],
  optional: string[] = [],
): Schema.Schema<Record<string, unknown>, unknown, never> =>
  Schema.Unknown.pipe(
    Schema.filter((value): value is Record<string, unknown> => typeof value === 'object' && value !== null, {
      message: message(`${label} must be an object`),
    }),
    Schema.filter((value) => {
      const record = value as Record<string, unknown>;
      const issues: ValidationIssue[] = [];
      for (const key of required) {
        if (typeof record[key] !== 'function') {
          issues.push(toIssue([key], `${label}.${key} must be a function`));
        }
      }
      for (const key of optional) {
        if (record[key] !== undefined && typeof record[key] !== 'function') {
          issues.push(toIssue([key], `${label}.${key} must be a function`));
        }
      }
      return issues.length > 0 ? issues : true;
    }),
  );

const cacheSchema = objectWithFunctionsSchema('cache', ['get', 'set']);

const leasesSchema = objectWithFunctionsSchema('leases', ['acquire', 'release'], ['markReady', 'isReady']);

const adapterTypeSchema = Schema.Union(
  Schema.Literal('memory'),
  Schema.Literal('redis'),
  Schema.Literal('memcached'),
  Schema.Literal('dynamodb'),
  Schema.Literal('mongodb'),
  Schema.Literal('postgres'),
  Schema.Literal('cloudflare-kv'),
  Schema.Literal('cloudflare-d1'),
  Schema.Literal('s3'),
  Schema.Literal('r2'),
);

const adapterConfigSchema = Schema.Struct({
  type: adapterTypeSchema,
  options: Schema.optional(Schema.Unknown),
});

const adapterInstanceSchema = Schema.Struct({
  cache: cacheSchema,
  leases: Schema.optional(leasesSchema),
});

const adapterSchema = Schema.Union(adapterConfigSchema, adapterInstanceSchema);

const abortSignalSchema = Schema.Unknown.pipe(
  Schema.filter(
    (value): value is AbortSignal =>
      typeof value === 'object' && value !== null && 'aborted' in value && typeof value.aborted === 'boolean',
    { message: message('signal must be an AbortSignal') },
  ),
);

const hooksSchema = objectWithFunctionsSchema('hooks', [], ['onHit', 'onLeader', 'onFollowerWait', 'onFallback']);

export const keySchema = nonEmptyStringSchema('key').pipe(Schema.brand('Key'));
export const ownerIdSchema = nonEmptyStringSchema('ownerId').pipe(Schema.brand('OwnerId'));

export const baseOptionsSchema = Schema.Struct({
  adapter: adapterSchema,
  leases: Schema.optional(leasesSchema),
  clock: Schema.optional(objectWithFunctionsSchema('clock', ['now'])),
  sleep: Schema.optional(functionSchema('sleep')),
  shouldCache: Schema.optional(predicateSchema('shouldCache')),
  ownerId: Schema.optional(ownerIdSchema),
  leaseTtl: Schema.optional(durationSchema('leaseTtl')),
  waitMax: Schema.optional(durationSchema('waitMax')),
  waitStep: Schema.optional(durationSchema('waitStep')),
  cacheTtl: Schema.optional(durationSchema('cacheTtl')),
  signal: Schema.optional(abortSignalSchema),
  waitStrategy: Schema.optional(functionSchema('waitStrategy')),
  hooks: Schema.optional(hooksSchema),
  validateOptions: Schema.optional(Schema.Boolean),
});

export const callOptionsSchema = Schema.Struct({
  cacheTtl: Schema.optional(durationSchema('cacheTtl')),
  leaseTtl: Schema.optional(durationSchema('leaseTtl')),
  waitMax: Schema.optional(durationSchema('waitMax')),
  waitStep: Schema.optional(durationSchema('waitStep')),
  shouldCache: Schema.optional(predicateSchema('shouldCache')),
  ownerId: Schema.optional(ownerIdSchema),
  signal: Schema.optional(abortSignalSchema),
  waitStrategy: Schema.optional(functionSchema('waitStrategy')),
  hooks: Schema.optional(hooksSchema),
});
export const fetcherSchema = functionSchema('fetcher');
export const waitDelaySchema = durationSchema('waitStrategy return');

export const formatParseIssues = (error: ParseResult.ParseError): ReadonlyArray<string> =>
  ParseResult.ArrayFormatter.formatErrorSync(error).map(
    (issue) => `${issue.path.length > 0 ? issue.path.map(String).join('.') : 'value'}: ${issue.message}`,
  );

export const decodeWith = <T, I>(
  schema: Schema.Schema<T, I, never>,
  value: unknown,
  label: string,
  context: CacheLockingErrorContext,
): Effect.Effect<T, ValidationError> => {
  const result = Schema.decodeUnknownEither(schema, { onExcessProperty: 'preserve' })(value);
  if (Either.isRight(result)) {
    return Effect.succeed(result.right);
  }
  const issues = formatParseIssues(result.left);
  return Effect.fail(
    new ValidationError(`${label} validation failed: ${issues.join('; ')}`, context, issues, result.left),
  );
};

export const decodeKey = (value: unknown, context: CacheLockingErrorContext): Effect.Effect<Key, ValidationError> =>
  decodeWith(keySchema, value, 'key', context);

export const decodeOwnerId = (
  value: unknown,
  context: CacheLockingErrorContext,
): Effect.Effect<OwnerId, ValidationError> => decodeWith(ownerIdSchema, value, 'ownerId', context);

export const decodeCache = (
  value: unknown,
  context: CacheLockingErrorContext,
): Effect.Effect<Cache<unknown>, ValidationError> =>
  decodeWith(cacheSchema, value, 'cache', context).pipe(Effect.map((cache) => cache as Cache<unknown>));

export const decodeLeases = (value: unknown, context: CacheLockingErrorContext): Effect.Effect<Leases, ValidationError> =>
  decodeWith(leasesSchema, value, 'leases', context).pipe(Effect.map((leases) => leases as Leases));
