# ADR 0003 — Process mechanism via watchexec-supervisor; policy stays local

- **Status:** Accepted for worker slots. The success criterion is not met.
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

Worker slots use `watchexec-supervisor` 6.0.0. `crates/supervisor/tests/process_compare.rs` compared it with `tokio::process` on the same child fixture: SIGUSR1, a READY handshake, exit status 7, process groups, and `pre_exec` file-descriptor inheritance. That behavior is not worse, so the slot spawn goes through `process_port`. The slot spawn does not create a new process group.

The success metric is not met. Rust `src/` without test files grew from 30 files and 10,363 lines at `b6a05ee` (1.3.0) to 44 files and 13,866 lines. `supervisor.rs` grew from 1,295 lines to 1,474. The memory policy, crash-loop, slot roster, and Redis publish stayed there, and supervisor telemetry was added. The custom slot spawn is gone. What remains of the local process mechanism, and why it stays:

- `spawn_metrics` is still `tokio::process::Command`. The metrics helper has no READY handshake, no drain signal, and no restart budget, so `watchexec-supervisor` does not replace it.
- `signal_child` still sends `SIGUSR1` and `SIGKILL` by pid. Drain and the hard memory limit are policy, not the spawn adapter.
- `simple-q-process` is the slot state machine. The supervisor loop does not call it yet, so deleting it would drop the tested policy without shrinking `supervisor.rs`.

Do not read “the custom process mechanism was deleted” as “there is less of our own code.” There is more.
