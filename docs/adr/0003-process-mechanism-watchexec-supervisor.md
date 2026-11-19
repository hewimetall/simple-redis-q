# ADR 0003 — Process mechanism via watchexec-supervisor; policy stays local

- **Status:** Accepted for worker slots
- **Date:** 2026-11-13

## Context

`supervisor.rs` (~1860 LOC) mixes spawn/wait/signals with memory policy, crash-loop, slot roster, and Redis publish. The brief requires checking `watchexec-supervisor` for mechanism (spawn/wait/signals/timeouts/process groups) while keeping policy local.

## Decision

1. Pin **`watchexec-supervisor` 6.0.0** (spike compiled and ran: `docs/research/spikes/RESULTS.md`).
2. If process tests prove equivalent or better reap/signal/group behavior, adopt it as `ProcessPort` adapter.
3. Retain local policy: slot allocation, crash-loop budget, RSS/cgroup thresholds, recycling reasons, supervisor Redis telemetry.
4. Do not move ACK/delivery decisions into the supervisor.

## Consequences

- Success metric: delete custom process mechanism without increasing lifecycle policy surface.
- Failure: keep tokio::process; document why in ADR amendment.
- Parent must still stop initializing CPython (separate workstream).

## Amendment

Worker slots use `watchexec-supervisor` 6.0.0. `crates/supervisor/tests/process_compare.rs` compared it with `tokio::process` on the same child fixture: SIGUSR1, a READY handshake, exit status 7, process groups, and `pre_exec` file-descriptor inheritance. That behavior is not worse, so the slot spawn goes through the crate. The metrics helper stays a direct `tokio::process` spawn. Policy stays in `supervisor.rs`. Signals are still delivered by pid. The slot spawn does not create a new process group.
