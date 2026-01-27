# Adapter Guidelines

## Overview

Rules for implementing cache and lease adapters.

## Client Usage

- Use official library types for adapters; do not define ad-hoc client shapes.
- Adapters receive configured clients; never construct SDK clients internally.

## Implementation Rules

- Implement `Cache<V>` and/or `Leases` from `src/core/types.ts`.
- TTL inputs are `effect` `Duration` values; guard negative TTLs with `Math.max(0, ...)`.
- Cache adapters own serialization, key prefixing, and TTL persistence.
- Lease adapters must be concurrency-safe (conditional/atomic operations).
- Defaults should be sensible and overridable via options.
