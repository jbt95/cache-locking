# Testing Guidelines

## Overview

Integration-first testing for adapters and providers.

## Rules

- Adapter tests are integration-only (no mocks).
- Gate integration tests via env vars:
  - `INTEGRATION_TESTS=1` enables container-backed tests.
  - `MINIFLARE_TESTS=1` enables Cloudflare tests.
- Prefer Testcontainers and Miniflare over mocks.
- Use unique prefixes/table names/buckets per test run.
