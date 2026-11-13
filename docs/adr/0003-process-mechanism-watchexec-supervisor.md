# ADR 0003 — Process mechanism via watchexec-supervisor; policy stays local

- **Status:** Proposed (spike-gated)
- **Date:** 2026-11-13

## Context

`supervisor.rs` (~1860 LOC) mixes spawn/wait/signals with memory policy, crash-loop, slot roster, and Redis publish. The brief requires checking `watchexec-supervisor` for mechanism (spawn/wait/signals/timeouts/process groups) while keeping policy local.

## Decision

1. Spike `watchexec-supervisor` (pin version in spike lockfile; crates.io observed 5.4.0 / docs 6.0.0).
2. If process tests prove equivalent or better reap/signal/group behavior, adopt it as `ProcessPort` adapter.
3. Retain local policy: slot allocation, crash-loop budget, RSS/cgroup thresholds, recycling reasons, supervisor Redis telemetry.
4. Do not move ACK/delivery decisions into the supervisor.

## Consequences

- Success metric: delete custom process mechanism without increasing lifecycle policy surface.
- Failure: keep tokio::process; document why in ADR amendment.
- Parent must still stop initializing CPython (separate workstream).
