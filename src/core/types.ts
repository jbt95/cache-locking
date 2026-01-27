import type { Brand, Duration, Effect, Option } from 'effect';
import type { AdapterConfig, ProviderAdapter } from '@adapters/factory';
import type { CacheLockingError } from '@core/errors';
import type { Phase } from '@core/phases';

export type Key = Brand.Branded<string, 'Key'>;
export type OwnerId = Brand.Branded<string, 'OwnerId'>;

export type Cache<V> = {
  get(key: string): Promise<V | null>;
  set(key: string, value: V, ttl?: Duration.DurationInput): Promise<void>;
};

export type LeaseRole = 'leader' | 'follower';

export type LeaseAcquireResult = {
  role: LeaseRole;
  leaseUntil: number;
};

export type LeaseReadyState = {
  ready: boolean;
  expired?: boolean;
};

export type Leases = {
  acquire(key: string, owner: string, ttl: Duration.DurationInput): Promise<LeaseAcquireResult>;
  release(key: string, owner: string): Promise<void>;
  markReady?(key: string): Promise<void>;
  isReady?(key: string): Promise<LeaseReadyState>;
};

export type CoreClock = {
  now(): number;
};

export type Sleep = (duration: Duration.DurationInput) => Promise<void>;

export type ShouldCache<V> = (value: V) => boolean;

export type WaitStrategyContext = {
  attempt: number;
  elapsed: Duration.Duration;
  remaining: Duration.Duration;
  waitMax: Duration.Duration;
  waitStep: Duration.Duration;
};

export type WaitStrategy = (context: WaitStrategyContext) => Duration.DurationInput;

export type CacheLockingHookResult = void | Promise<void>;

export type CacheLockingHooks<V> = {
  onHit?: (value: V, context: { key: string }) => CacheLockingHookResult;
  onLeader?: (value: V, context: { key: string; leaseUntil: number; cached: boolean }) => CacheLockingHookResult;
  onFollowerWait?: (context: {
    key: string;
    leaseUntil: number;
    waited: Duration.Duration;
    outcome: 'HIT' | 'FALLBACK';
  }) => CacheLockingHookResult;
  onFallback?: (
    value: V,
    context: { key: string; leaseUntil: number; waited: Duration.Duration },
  ) => CacheLockingHookResult;
};

export type PhaseAdapter = 'cache' | 'leases' | 'fetcher' | 'hooks' | 'wait' | 'validation';

export type CacheLockingErrorContext = {
  key?: string;
  phase: Phase;
  adapter?: PhaseAdapter;
};

export type CacheLockingErrorCode = CacheLockingError['_tag'];

export const CacheOutcome = {
  HIT: 'HIT',
  MISS_LEADER: 'MISS-LEADER',
  MISS_LEADER_NOCACHE: 'MISS-LEADER-NOCACHE',
  MISS_FOLLOWER_HIT: 'MISS-FOLLOWER-HIT',
  MISS_FOLLOWER_FALLBACK: 'MISS-FOLLOWER-FALLBACK',
} as const;

export type CacheOutcome = (typeof CacheOutcome)[keyof typeof CacheOutcome];

export type CacheMeta = {
  cache: CacheOutcome;
  leaseUntil?: number;
  waited?: Duration.Duration;
};

export type GetOrSetResult<V> = {
  value: V;
  meta: CacheMeta;
};

export type FetcherContext = { signal?: AbortSignal };
export type FetcherWithContext<V> = (context?: FetcherContext) => Promise<V>;
export type Fetcher<V> = FetcherWithContext<V>;

export type GetOrSetOptions<V> = {
  cacheTtl?: Duration.DurationInput;
  leaseTtl?: Duration.DurationInput;
  waitMax?: Duration.DurationInput;
  waitStep?: Duration.DurationInput;
  shouldCache?: ShouldCache<V>;
  ownerId?: string;
  signal?: AbortSignal;
  waitStrategy?: WaitStrategy;
  hooks?: CacheLockingHooks<V>;
};

export type CacheLockingOptions<V> = {
  adapter: AdapterConfig<V> | ProviderAdapter<V>;
  leases?: Leases;
  clock?: CoreClock;
  sleep?: Sleep;
  validateOptions?: boolean;
} & GetOrSetOptions<V>;

export type ValidatedGetOrSetOptions<V> = Omit<GetOrSetOptions<V>, 'ownerId'> & {
  cacheTtl?: Duration.Duration;
  leaseTtl?: Duration.Duration;
  waitMax?: Duration.Duration;
  waitStep?: Duration.Duration;
  ownerId?: OwnerId;
};
export type ValidatedCacheLockingOptions<V> = Omit<CacheLockingOptions<V>, 'ownerId' | 'adapter' | 'leases'> & {
  cache: Cache<V>;
  leases: Leases;
  cacheTtl?: Duration.Duration;
  leaseTtl?: Duration.Duration;
  waitMax?: Duration.Duration;
  waitStep?: Duration.Duration;
  ownerId?: OwnerId;
};

export type GetOrSetFn<V> = (
  key: string,
  fetcher: Fetcher<V>,
  options?: GetOrSetOptions<V>,
) => Promise<GetOrSetResult<V>>;

export type GetOrSetEffectFn<V> = (
  key: string,
  fetcher: Fetcher<V>,
  options?: GetOrSetOptions<V>,
) => Effect.Effect<GetOrSetResult<V>, CacheLockingError>;

export type CacheLocking<V> = {
  getOrSet: GetOrSetFn<V>;
  cache: Cache<V>;
  leases: Leases;
};

export type CacheLockingEffect<V> = {
  getOrSet: GetOrSetEffectFn<V>;
  cache: Cache<V>;
  leases: Leases;
};

export type ResolvedDefaults<V> = {
  leaseTtl: Duration.Duration;
  waitMax: Duration.Duration;
  waitStep: Duration.Duration;
  shouldCache: ShouldCache<V>;
  cacheTtl?: Duration.Duration;
  ownerId: OwnerId;
  signal?: AbortSignal;
  waitStrategy: WaitStrategy;
  hooks?: CacheLockingHooks<V>;
};

export type ResolvedOptions<V> = {
  leaseTtl: Duration.Duration;
  waitMax: Duration.Duration;
  waitStep: Duration.Duration;
  shouldCache: ShouldCache<V>;
  cacheTtl?: Duration.Duration;
  ownerId: OwnerId;
  signal?: AbortSignal;
  waitStrategy: WaitStrategy;
  hooks?: CacheLockingHooks<V>;
};

export type WaitResult<V> = {
  value: Option.Option<V>;
  waited: Duration.Duration;
};
export type WaitOutcome<V> = WaitResult<V> & {
  outcome: 'HIT' | 'FALLBACK';
};
