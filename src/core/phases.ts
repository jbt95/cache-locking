/** Cache locking phase identifiers for tracing and errors. */
export const Phase = {
  Validation: 'validation',
  CacheGet: 'cache.get',
  CacheSet: 'cache.set',
  LeaseAcquire: 'leases.acquire',
  LeaseRelease: 'leases.release',
  LeaseMarkReady: 'leases.markReady',
  LeaseIsReady: 'leases.isReady',
  Fetcher: 'fetcher',
  HooksOnHit: 'hooks.onHit',
  HooksOnLeader: 'hooks.onLeader',
  HooksOnFollowerWait: 'hooks.onFollowerWait',
  HooksOnFallback: 'hooks.onFallback',
  WaitStrategy: 'waitStrategy',
  WaitSleep: 'wait.sleep',
  Abort: 'abort',
} as const;

/** Union of phase identifiers. */
export type Phase = (typeof Phase)[keyof typeof Phase];
