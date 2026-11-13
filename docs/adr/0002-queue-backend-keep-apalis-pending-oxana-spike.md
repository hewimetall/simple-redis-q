# ADR 0002 — Queue backend: keep Apalis; Oxana only after spike

- **Status:** Accepted (provisional)
- **Date:** 2026-11-13
- **Baseline:** Apalis / apalis-redis **1.1.1** in `v1.2.0` lockfile

## Context

The brief requires comparing retention of Apalis with migration to Oxana. Migration must not be presumed. Oxana is Redis-depth focused but centers on typed Rust workers; our handlers are Python asyncio behind a bridge.

## Decision

1. **Default:** keep Apalis Redis as the queue adapter behind a new `QueuePort`.
2. Extract custom delivery/poison/unique-key/business-lock code as explicit modules owned by us — do not pretend Apalis covers them.
3. Run an Oxana **spike** (question/hypothesis/experiment/criterion in `docs/research/candidates.md`) before any production migration.
4. **Stop migration** if Oxana needs more workarounds or weakens claim/ACK/lease guarantees; record outcome in a superseding ADR.

## Consequences

- Stage C vertical slice uses Apalis adapter.
- Contract suite is written against `QueuePort` semantics so a future Oxana adapter can be compared fairly.
- No second parallel production backend “just in case”.
