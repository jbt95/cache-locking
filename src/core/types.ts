import type { Brand, Duration, Effect, Option } from 'effect';
import type { AdapterConfig, ProviderAdapter } from '@adapters/factory';
import type { AdapterError, CacheLockingError } from '@core/errors';
import type { Phase } from '@core/phases';

/** Branded cache key. */
export type Key = Brand.Branded<string, 'Key'>;
/** Branded lease owner id. */
export type OwnerId = Brand.Branded<string, 'OwnerId'>;

/** Cache adapter interface. */
export type Cache<V> = {
  get(key: string): Effect.Effect<V | null, AdapterError>;
  set(key: string, value: V, ttl?: Duration.DurationInput): Effect.Effect<void, AdapterError>;
};

/** Lease holder role. */
export type LeaseRole = 'leader' | 'follower';

/** Result for lease acquisition. */
export type LeaseAcquireResult = {
  role: LeaseRole;
  leaseUntil: number;
};

/** Lease readiness state. */
export type LeaseReadyState = {
  ready: boolean;
  expired?: boolean;
};

/** Lease adapter interface. */
export type Leases = {
  acquire(key: string, owner: string, ttl: Duration.DurationInput): Effect.Effect<LeaseAcquireResult, AdapterError>;
  release(key: string, owner: string): Effect.Effect<void, AdapterError>;
  markReady?(key: string): Effect.Effect<void, AdapterError>;
  isReady?(key: string): Effect.Effect<LeaseReadyState, AdapterError>;
};

/** Clock abstraction for deterministic timing. */
export type CoreClock = {
  now(): number;
};

/** Sleep function for waits. */
export type Sleep = (duration: Duration.DurationInput) => Effect.Effect<void>;

/** Cache predicate for caching decisions. */
export type ShouldCache<V> = (value: V) => boolean;

/** Context for wait strategy calculations. */
export type WaitStrategyContext = {
  attempt: number;
  elapsed: Duration.Duration;
  remaining: Duration.Duration;
  waitMax: Duration.Duration;
  waitStep: Duration.Duration;
};

/** Wait strategy returns a delay duration. */
export type WaitStrategy = (context: WaitStrategyContext) => Duration.DurationInput;

/** Hook effect helper type. */
export type HookEffect<E = never, R = never> = Effect.Effect<void, E, R>;
/** Hook return type for Effect-based hooks. */
export type CacheLockingHookResult<E = never, R = never> = HookEffect<E, R>;

/** Hook callbacks invoked during cache locking phases. */
export type CacheLockingHooks<V, E = never, R = never> = {
  onHit?: (value: V, context: { key: string }) => CacheLockingHookResult<E, R>;
  onLeader?: (value: V, context: { key: string; leaseUntil: number; cached: boolean }) => CacheLockingHookResult<E, R>;
  onFollowerWait?: (context: {
    key: string;
    leaseUntil: number;
    waited: Duration.Duration;
    outcome: 'HIT' | 'FALLBACK';
  }) => CacheLockingHookResult<E, R>;
  onFallback?: (
    value: V,
    context: { key: string; leaseUntil: number; waited: Duration.Duration },
  ) => CacheLockingHookResult<E, R>;
};

/** Adapter category for error context. */
export type PhaseAdapter = 'cache' | 'leases' | 'fetcher' | 'hooks' | 'wait' | 'validation';

/** Error context for cache locking failures. */
export type CacheLockingErrorContext = {
  key?: string;
  phase: Phase;
  adapter?: PhaseAdapter;
};

/** Error tag codes for cache locking failures. */
export type CacheLockingErrorCode = CacheLockingError['_tag'];

/** Cache outcomes for getOrSet requests. */
export const CacheOutcome = {
  HIT: 'HIT',
  MISS_LEADER: 'MISS-LEADER',
  MISS_LEADER_NOCACHE: 'MISS-LEADER-NOCACHE',
  MISS_FOLLOWER_HIT: 'MISS-FOLLOWER-HIT',
  MISS_FOLLOWER_FALLBACK: 'MISS-FOLLOWER-FALLBACK',
} as const;

/** Union of cache outcome values. */
export type CacheOutcome = (typeof CacheOutcome)[keyof typeof CacheOutcome];

/** Metadata describing cache outcome and timing. */
export type CacheMeta = {
  cache: CacheOutcome;
  leaseUntil?: number;
  waited?: Duration.Duration;
};

/** Result for cache locking calls. */
export type GetOrSetResult<V> = {
  value: V;
  meta: CacheMeta;
};

/** Fetcher invocation context. */
export type FetcherContext = { signal?: AbortSignal };
/** Effect fetcher signature. */
export type Fetcher<V, E = never, R = never> = (context?: FetcherContext) => Effect.Effect<V, E, R>;

/** Per-call overrides for getOrSet. */
export type GetOrSetOptions<V, E = never, R = never> = {
  cacheTtl?: Duration.DurationInput;
  leaseTtl?: Duration.DurationInput;
  waitMax?: Duration.DurationInput;
  waitStep?: Duration.DurationInput;
  shouldCache?: ShouldCache<V>;
  ownerId?: string;
  signal?: AbortSignal;
  waitStrategy?: WaitStrategy;
  hooks?: CacheLockingHooks<V, E, R>;
};

/** Adapter inputs accepted by cache locking. */
export type AdapterInput<V> = AdapterConfig<V> | ProviderAdapter<V> | 'memory';

/** Global options for cache locking initialization. */
export type CacheLockingOptions<V, E = never, R = never> = {
  adapter: AdapterInput<V>;
  leases?: Leases;
  clock?: CoreClock;
  sleep?: Sleep;
  validateOptions?: boolean;
} & GetOrSetOptions<V, E, R>;

/** One-shot getOrSet options, including key and fetcher. */
export type GetOrSetOnceOptions<
  V,
  EFetcher = never,
  RFetcher = never,
  EHooks = never,
  RHooks = never,
> = CacheLockingOptions<V, EHooks, RHooks> & {
  key: string;
  fetcher: Fetcher<V, EFetcher, RFetcher>;
};

/** Validated options for getOrSet calls. */
export type ValidatedGetOrSetOptions<V, E = never, R = never> = Omit<GetOrSetOptions<V, E, R>, 'ownerId'> & {
  cacheTtl?: Duration.Duration;
  leaseTtl?: Duration.Duration;
  waitMax?: Duration.Duration;
  waitStep?: Duration.Duration;
  ownerId?: OwnerId;
};
/** Validated options for cache locking initialization. */
export type ValidatedCacheLockingOptions<V, E = never, R = never> = Omit<
  CacheLockingOptions<V, E, R>,
  'ownerId' | 'adapter' | 'leases'
> & {
  cache: Cache<V>;
  leases: Leases;
  cacheTtl?: Duration.Duration;
  leaseTtl?: Duration.Duration;
  waitMax?: Duration.Duration;
  waitStep?: Duration.Duration;
  ownerId?: OwnerId;
};

/** Effect-based getOrSet signature with typed errors. */
export type GetOrSetEffectFn<V, EH = never, RH = never> = {
  <EF = never, RF = never, EH2 = never, RH2 = never>(
    key: string,
    fetcher: Fetcher<V, EF, RF>,
    options?: GetOrSetOptions<V, EH2, RH2>,
  ): Effect.Effect<GetOrSetResult<V>, CacheLockingError | EH | EH2 | EF, RH | RH2 | RF>;
};

/** Effect-based cache locking client surface. */
export type CacheLockingEffect<V, EH = never, RH = never> = {
  getOrSet: GetOrSetEffectFn<V, EH, RH>;
  cache: Cache<V>;
  leases: Leases;
};

/** Resolved defaults for cache locking calls. */
export type ResolvedDefaults<V, E = never, R = never> = {
  leaseTtl: Duration.Duration;
  waitMax: Duration.Duration;
  waitStep: Duration.Duration;
  shouldCache: ShouldCache<V>;
  cacheTtl?: Duration.Duration;
  ownerId: OwnerId;
  signal?: AbortSignal;
  waitStrategy: WaitStrategy;
  hooks?: CacheLockingHooks<V, E, R>;
};

/** Resolved runtime options for a getOrSet call. */
export type ResolvedOptions<V> = {
  leaseTtl: Duration.Duration;
  waitMax: Duration.Duration;
  waitStep: Duration.Duration;
  shouldCache: ShouldCache<V>;
  cacheTtl?: Duration.Duration;
  ownerId: OwnerId;
  signal?: AbortSignal;
  waitStrategy: WaitStrategy;
};

/** Wait result with elapsed duration. */
export type WaitResult<V> = {
  value: Option.Option<V>;
  waited: Duration.Duration;
};
/** Wait result with outcome tag. */
export type WaitOutcome<V> = WaitResult<V> & {
  outcome: 'HIT' | 'FALLBACK';
};
