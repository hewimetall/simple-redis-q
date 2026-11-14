# Candidate components — pinned audit

Spike sources: `docs/research/spikes/`. Results: `docs/research/spikes/RESULTS.md`.

## Versions (pinned 2026-11-13)

| Crate | Pin | crates.io default | License | Notes |
| --- | --- | --- | --- | --- |
| `apalis` | **1.1.1** | 1.1.1 | MIT OR Apache-2.0 | lockfile at `v1.2.0` |
| `apalis-redis` | **1.1.1** | 1.1.1 | MIT | checksum `39338ae8…` |
| `watchexec-supervisor` | **6.0.0** | 6.0.0 | Apache-2.0 OR MIT | spike compiled+ran; 5.4.x obsolete for us |
| `oxana` | **2.5.0** | 2.5.0 | MIT | opaque Worker spike compiled |

## 1. Apalis Redis (incumbent — default)

| Need | Evidence | Our tests |
| --- | --- | --- |
| Atomic claim | `lock_tasks(true)` + Lua in apalis-redis 1.1.1 | poll_interval, orphan (green on Redis 7.0.15) |
| Renew / orphan | `heartbeat_interval` × `missed_heartbeats` | orphan_integration |
| Finish | worker `Result` / `AbortError` | runtime_contract |
| Delayed | schedule / delay enqueue | partial |
| Dedup | idempotency SETNX | enqueue tests |
| Delivery/poison | **custom** | keep |

**Fetch-loop ownership:** Apalis `WorkerBuilder` — matches binding `QueuePort` (backend-owned loop).

## 2. Oxana 2.5.0

| Need | Evidence from crate sources / spike |
| --- | --- |
| Opaque Python jobs | **Compile OK:** one `OpaqueJob { message: Value }` + one `Worker` (`spikes/oxana-2.5.0`) |
| Claim | `dispatcher::claim_job` — LMOVE into worker claim list |
| Heartbeat / resurrect | process ZSET + `resurrect_claim.lua` |
| Finish | executor `finish_*` Lua; tightly coupled to Oxana runtime |
| Storage-only use | **Poor fit:** claim/finish are inside dispatcher/executor, not a thin port |

**Stop condition (kept):** do not migrate unless ownership contract suite shows clear win over Apalis **and** Python bridge sits cleanly inside one Oxana Worker without re-implementing claim.

## 3. watchexec-supervisor 6.0.0

| API used in spike | Role |
| --- | --- |
| `start_job` / `Job` | mechanism handle |
| `signal(Signal::User1)` | drain analog |
| `stop` / `to_wait` / `delete_now` | stop + reap |
| `Program::Exec` | child argv |

Policy (slots, memory, crash-loop, Redis supervisor record) stays local.

## Decision snapshot

| Topic | Decision |
| --- | --- |
| Queue | **Keep Apalis 1.1.1** behind QueuePort |
| Process mechanism | **Prefer watchexec-supervisor 6.0.0** after process tests |
| Oxana | Opaque Worker possible; full migration **not** selected |
