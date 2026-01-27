# TypeScript Guidelines

## Overview

Type safety rules for the cache-locking library and adapters.

## Type Safety

- Keep `strict` assumptions; avoid `any` and prefer `unknown` with narrowing.
- Use `interface` for object shapes and `type` for unions or type-level composition.
- Use `satisfies` to validate config objects while preserving literal types.
- Avoid unsafe `as` assertions; prefer type guards or runtime decoding.

## Advanced Types

- Use generics for adapters and public APIs; keep constraints minimal and explicit.
- Use conditional types with `infer` for helper types; avoid deep recursion that slows builds.
- Use mapped and utility types (`Pick`, `Omit`, `Partial`, `Record`) to derive API shapes.
- Use template literal types when modeling key patterns or string unions.

## API Surface

- Keep exported types stable and documented; avoid leaking internal helper types.
- Prefer `readonly` arrays/objects when values are immutable.
