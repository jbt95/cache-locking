# Architecture & Structure

## Overview

Defines where core logic, adapters, and tests live.

## Structure

- `src/core/*`: cache-locking engine (validation, phases, runtime, hooks).
- `src/adapters/*`: provider implementations; one file per provider.
- `src/adapters/factory.ts`: adapter configs + factories for `createCacheLocking`.
- `test/*`: unit tests; `test/integration/*`: real integration tests.
