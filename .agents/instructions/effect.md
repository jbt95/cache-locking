# Effect Best Practices

## Overview

Consistent Effect patterns for services, errors, layers, and options.

## Services & Layers

- Use `Effect.Service` with `accessors: true` for business logic services.
- Declare dependencies in the service definition; compose layers at the app root.
- Use `Context.Tag` only for infrastructure/runtime resources that are injected externally.

## Errors & Options

- Define errors with `Schema.TaggedError` and keep error tags specific.
- Handle errors with `Effect.catchTag`/`Effect.catchTags`; avoid `catchAll` that erases tags.
- Prefer `Option` over `null`/`undefined`; handle both cases explicitly.

## Functions & Logging

- Wrap service methods with `Effect.fn("Service.method")` for tracing.
- Use `Effect.log` with structured data; avoid `console.log`.
- Use `Config.*` for env/config values when needed, not `process.env` in core code.

## Anti-Patterns

- Do not call `Effect.runSync`/`Effect.runPromise` inside services; only at app boundaries.
