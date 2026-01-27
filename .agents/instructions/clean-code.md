# Clean Code Practices

## Overview

Maintainable, testable code conventions across core and adapters.

## Rules

- Keep functions small and focused; extract helpers for complex flows.
- Prefer early returns to reduce nesting and highlight main paths.
- Name by domain intent (`leaseTtl`, `cacheKeyPrefix`); avoid generic names.
- Keep side effects at the edges (adapters); core logic should be mostly pure.
- Avoid hidden coupling; pass dependencies explicitly rather than using globals.
- Comment only when explaining non-obvious invariants or tricky logic.
