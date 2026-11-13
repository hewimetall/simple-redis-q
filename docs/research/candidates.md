# Candidate components — audit notes

Spike rule (brief §15.3): before each spike record `question → hypothesis → experiment → decision criterion`. Temporary code stays out of production crates.

## 1. Apalis Redis (incumbent)

| Field | Value |
| --- | --- |
| Versions in use | `apalis` / `apalis-redis` **1.1.1** (`Cargo.lock` at `v1.2.0`) |
| License | MIT OR Apache-2.0 / MIT |
| Role today | claim, heartbeat, orphan requeue, ack/abort, delayed schedule, idempotency SETNX, board |

### Capability checklist (docs + current usage; not yet re-proven by new contract suite)

| Need | Incumbent evidence | Gap |
| --- | --- | --- |
| Atomic claim | `lock_tasks(true)` | dual-worker contract |
| Ownership renew | heartbeat_interval + missed_heartbeats | A/B renewal-off diagnostic |
| ACK fencing | storage finish APIs | stale ACK after re-claim |
| Delayed jobs | schedule / delay enqueue | boundary tests |
| Dedup | idempotency key (used) | separate from business lock (already) |
| Delivery counters | **custom** | keep |
| Cancellation safety | worker JoinHandles | TaskTracker drain |
| Redis 7/8 | CI | OK |
| Adapter complexity | medium; already integrated | extract behind port |

### Upstream issue chain template

```text
issue → change → commit → release → used version → our regression test
```

No new upstream dependency change in Stage A. When bumping Apalis, fill this chain; popularity alone is insufficient.

### Decision lean (research only)

**Default path: keep Apalis**, extract `QueuePort`, delete leakage of Redis/Apalis types from core. Revisit only if contract suite shows ownership holes Apalis cannot close without larger patches than Oxana adapter.

---

## 2. Oxana

| Field | Value |
| --- | --- |
| Repo | `https://github.com/pragmaplatform/oxana` |
| License | MIT |
| Observed versions | crates.io ~2.1.10 listing; docs quick-start cites 2.5.0 — **pin in spike** |
| Fit | Redis-only job library with unique/resilient jobs and dashboard |

### Fit analysis

- Oxana workers are **Rust-typed**. Our handlers are **Python asyncio**. Full Oxana runtime would either (a) force Rust wrappers per job type, or (b) reduce Oxana to storage/claim while we keep Python execution — possibly duplicating executor concerns.
- Unique/resumable/cron features overlap product needs but also pull macro/registry API foreign to current `Job` JSON messages.
- Smaller community / download volume than Apalis; production use at Player.gg is claimed but not a substitute for our contract suite.

### Spike plan (Stage B→C gate)

```text
question: Can Oxana provide atomic claim + renew + ack fencing for opaque JSON jobs
          without embedding our Python executor inside Oxana workers?
hypothesis: Storage APIs allow enqueue/claim/ack for bytes/JSON with lease semantics
            comparable to Apalis lock_tasks.
experiment: minimal Rust binary; Redis; two claimants; long handler lease; stale ack.
criterion:  equal or better fencing with ≤ current custom Redis LOC outside Python bridge;
            else STOP migration (ADR).
```

**Stop condition:** more workarounds or weaker guarantees than Apalis → keep Apalis.

---

## 3. watchexec-supervisor

| Field | Value |
| --- | --- |
| Crate | `watchexec-supervisor` |
| Observed version | crates.io **5.4.0**; docs.rs also **6.0.0** — pin in spike |
| License | Apache-2.0 OR MIT |
| API | `start_job` → `Job` controls: start/stop/restart/signal/wait; process groups |

### Fit analysis

- Matches brief: **mechanism separate from policy**.
- Does not provide crash-loop budgets, RSS/cgroup policy, slot allocation, or Redis supervisor records — we keep those.
- Replaces custom sequencing around `tokio::process::Child`, signal delivery, and wait — largest mechanism share of `supervisor.rs`.

### Spike plan

```text
question: Can Job + process-wrap express SIGUSR1 drain, SIGKILL escalate, and reap
          with process-group cleanup for our child agent-worker?
hypothesis: ordered controls + Signal::User1/Terminate/Kill cover mechanism;
            policy remains in our SupervisorPolicy.
experiment: spawn sleep/trap scripts; drain; kill; verify wait exit codes; group cleanup.
criterion:  delete custom spawn/wait/signal paths without new shared-memory requirements;
            process tests green.
```

---

## 4. Tokio graceful shutdown / TaskTracker

Not a third-party product swap; pattern adoption inside `runtime` crate.

```text
question: Can admission close + TaskTracker wait separate fetch cancel from ACK/retry?
hypothesis: yes, if every long-lived task is registered and tracker closed only after admission stop.
experiment: unit with fake QueuePort + barrier fault injection.
criterion: cancel fetch leaves in-flight finish tasks running until complete or budget.
```

---

## 5. Comparison snapshot

| Dimension | Keep Apalis | Migrate Oxana | watchexec-supervisor |
| --- | --- | --- | --- |
| Queue claim/ACK | incumbent | undecided spike | n/a |
| Python handler host | our bridge | still our bridge | n/a |
| Process spawn/wait | custom today | n/a | primary candidate |
| Risk of more custom lifecycle | low if ported | **high** if forced | should **reduce** mechanism LOC |
| Stage A decision | **prefer keep** | spike-gated | **prefer adopt** after spike |

Migration to Oxana is **not** predetermined.
