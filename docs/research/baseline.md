# Baseline research — simple-redis-q architectural fork

**Stage:** A (research)  
**Status:** confirmed facts vs hypotheses are marked explicitly  
**Baseline release:** `v1.2.0`  
**Research date (UTC):** 2026-11-13  
**Branch:** `cursor/arch-fork-research-ad8c`

This document records the pinned baseline for the architectural fork. Results from `1.0.0rc7` or the bench stand in `hewimetall/resurche_for_baground_tasks` are cited only as prior context and are **not** claimed as `v1.2.0` evidence.

## 1. Pinned revisions

| Artifact | Ref | Full SHA |
| --- | --- | --- |
| simple-redis-q release | tag `v1.2.0` (annotated) | `9164eeab0361639d8cd2d0ca2bb91c355710c1a8` |
| simple-redis-q prior RC (context only) | tag `v1.0.0rc7` | `47cc60ea4df585d48c4e0eff56660a978b646a95` |
| resurche_for_baground_tasks PR #1 head | `cursor/queue-bench-69c9` | `c81f29f7aad4d80b5d2f62ceaadcf21abd9f8a0b` |
| resurche_for_baground_tasks PR #1 base | `main` | `b4bf4b897325eb6152b851041aeb870edc4d4ad2` |

Lockfile digests at `v1.2.0`:

| File | SHA-256 |
| --- | --- |
| `Cargo.lock` | `b0d84b365af89405c543e9c8b6b7fae057a379f545d1a7b2d18ada46bfe2e617` |
| `uv.lock` | `0fdedf66fd2088877fd933b4bcd26c7b1495f89cdf5fcdc5aa468e3147da1a1b` |

Build parameters recorded from repository files (not from a fresh wheel build in this research VM):

- Rust toolchain: `rust-toolchain.toml` → channel `1.99.1`, components `rustfmt`, `clippy`
- Workspace `rust-version`: `1.99`
- Python packaging: `requires-python >= 3.12`; maturin `>=1.16,<2`
- CI matrix (declared): CPython `3.12`–`3.15` × `ubuntu-24.04` / `ubuntu-24.04-arm`
- Wheel target (declared): Linux x86_64 and aarch64, GIL builds, `manylinux_2_28`
- Redis in CI: `redis:7` (6379) and `redis:8` (6380)

## 2. Research environment (this agent VM)

| Component | Observed |
| --- | --- |
| OS | Linux 6.12.113+ x86_64, glibc 2.39 (`Linux-6.12.113+-x86_64-with-glibc2.39`) |
| rustc / cargo | `1.99.1` (auto-installed to match toolchain file) |
| Python | `3.12.3` |
| Redis server | **absent** in this VM — integration/contract Redis proofs deferred |
| Working tree | clean checkout of `v1.2.0` on branch `cursor/arch-fork-research-ad8c` |

## 3. License audit

| Component | License | Evidence | Risk |
| --- | --- | --- | --- |
| `hewimetall/simple-redis-q` | **unspecified** | GitHub API `/license` → null; no `LICENSE*` file in tree at `v1.2.0` | Fork/redistribution needs explicit license decision (ADR recommended) |
| `apalis` 1.1.1 | MIT OR Apache-2.0 | crates.io | Compatible with typical open-source redistribution |
| `apalis-redis` 1.1.1 | MIT | crates.io | Same |
| `apalis-board` 1.0.0 | (via apalis ecosystem) | Cargo.lock present | Confirm at integration time |
| `oxana` 2.x | MIT | crates.io / GitHub | Compatible |
| `watchexec-supervisor` | Apache-2.0 OR MIT (crate); docs note Apache-2.0 | crates.io / docs.rs | Compatible |
| PyO3 / redis / tokio | Apache-2.0 / MIT family | crates.io | Compatible |

**Confirmed:** source project has no declared SPDX license at `v1.2.0`.  
**Hypothesis:** maintainers intend permissive terms; not proven.

## 4. Current layout at `v1.2.0`

```text
crates/
  agent-worker/     # binary: CLI, supervisor, child, bridge, worker, telemetry (~1860 LOC supervisor alone)
  simple-q-runtime/ # PyO3 extension + Redis/admin/config/job/lock (~2550 LOC admin alone)
python/simple_q/    # public Python API
python/tests/       # mixed unit + integration (~3160 LOC)
docs/               # user/architecture docs for 1.2
.github/workflows/  # CI: unit, integration(+Redis), wheel
```

Approximate Rust source size (non-test): ~12k LOC across both crates. Largest concentration of mixed concerns: `supervisor.rs`, `admin.rs`, `worker.rs`, `telemetry.rs`.

### Delivery model (confirmed from docs + code)

1. Producer: Python `Client` / CLI enqueue → Apalis Redis storage (`SETNX` idempotency when `unique_key`).
2. Parent `agent-worker worker`: imports App via PyO3 to read Redis URL + `Config`, then forks children.
3. Child: initializes CPython, runs Apalis workers, `handle_job` → Python `App.dispatch`, then ACK/retry/dead via Apalis + custom delivery/idempotency keys.
4. Supervisor: spawn/reap, SIGUSR1 drain, memory RSS/cgroup policy, Redis supervisor record, anonymous memfd busy-digit.

### Hard-constraint gap (confirmed)

Target contract: **Rust parent must not initialize CPython; CPython only in child.**

At `v1.2.0`, parent path `Command::Worker { child: false }` calls `runtime::initialize()` then `bridge::read_client` before `supervisor::run` (`main.rs`). That initializes the interpreter in the parent to discover Redis URL/config from the Python App. Children also call `runtime::initialize()`.

This is a primary Stage C gap: composition must obtain Redis URL / config without parent CPython (env, config file, or lightweight non-Py bridge).

## 5. Locked dependency versions (from `Cargo.lock` at `v1.2.0`)

| Crate | Version | Checksum (prefix) |
| --- | --- | --- |
| `apalis` | 1.1.1 | `26230170dee9…` |
| `apalis-core` | 1.1.1 | (lockfile) |
| `apalis-redis` | 1.1.1 | `39338ae8ab10…` |
| `apalis-board` | 1.0.0 | (lockfile) |
| `pyo3` | 0.30.1 | |
| `pyo3-async-runtimes` | 0.30.0 | |
| `redis` | 1.8.1 | |
| `tokio` | 1.54.1 | |

**Not present in tree:** `oxana`, `watchexec-supervisor`.

## 6. External research sources

### 6.1 `hewimetall/resurche_for_baground_tasks` PR #1

- Title: stand and report comparing simple-q with Dramatiq, Celery, arq, taskiq.
- Measured against **simple-q `1.0.0rc7`**, not `v1.2.0`.
- Useful findings (prior context only):
  - Long-control 2400 s can pass with one worker / one attempt; that is **not** recovery or dual-worker ownership proof.
  - Harness bugs: `kill(pid,0)` treats zombies as live; SIGKILL after grace without proper `poll`/`wait`; spans can glue two executions.
  - simple-q empty fetch waits on heartbeat when idle (pre-`poll_with_interval`); **`v1.2.0` changed idle fetch to `SIMPLE_Q_POLL_INTERVAL_MS`** — do not reuse rc7 latency numbers.
- Stand remediation requirements from the architectural brief (Section 11) remain open for any future reuse of that harness.

### 6.2 Apalis / apalis-redis (candidate: keep)

**Confirmed from crates.io / docs (2026-11):**

- `apalis` latest observed: **1.1.1** (matches lockfile).
- `apalis-redis` crates.io listing also shows **1.0.0** line and docs.rs may show newer patch lines; **this project uses 1.1.1** per `Cargo.lock`.
- Features used today: `lock_tasks(true)`, heartbeat + missed_heartbeats orphan recovery, `poll_with_interval`, idempotency SETNX, task-tracking metadata, persist_results, board UI.
- Custom code layered on top: delivery counter, poison policy, unique-key terminal release, `started_at` vs `locked_at`, business lock with owner-checked renew/release, supervisor admin snapshot.

**Hypothesis (needs contract tests before any migration):** Apalis fencing on ACK after ownership loss is sufficient for our outcomes; must be proven with dual-worker + A/B renewal suite, not assumed from docs.

### 6.3 Oxana (candidate: migrate — undecided)

**Confirmed:**

- Repo: `pragmaplatform/oxana`, MIT, Redis-only depth focus.
- crates.io versions observed in research: listings around **2.1.x–2.5.0** (docs.rs quick-start cites 2.5.0). Pin exact version only after a spike with `Cargo.lock`.
- Features claimed: unique jobs, retries, scheduled/cron, resilient jobs, graceful shutdown, dashboard, Prometheus.
- Macro/registry-heavy API; workers are typed Rust handlers — **does not natively host Python handlers**. Adapter would still need our Python bridge outside Oxana's executor, or Oxana used only as storage/claim layer.

**Hypothesis:** using Oxana as full runtime would fight the Python-child model; using it only as Redis job store may require more adapter surface than keeping Apalis. **Migration not decided.** Stop condition: if adapter + patches exceed current Apalis custom layer without stronger ownership proofs → keep Apalis (ADR).

### 6.4 `watchexec-supervisor` (candidate: process mechanism)

**Confirmed:**

- crates.io latest observed in research fetch: **5.4.0** (Apache-2.0 OR MIT); docs.rs also indexes **6.0.0** — version to pin via spike.
- Provides `Job` handle: start/stop/restart/signal/wait; process groups via `process-wrap`; ordered control queue with tickets.
- Explicitly mechanism-oriented; application policy (slots, crash-loop, memory) stays ours.

**Hypothesis:** can replace much of custom spawn/wait/signal sequencing in `supervisor.rs` while retaining memory/crash-loop policy. Spike required before deleting custom code.

## 7. Confirmed vs hypothesis summary

| Statement | Class |
| --- | --- |
| Baseline SHA is `9164eea…` (`v1.2.0`) | Confirmed |
| Parent initializes CPython to read App client | Confirmed |
| Queue backend is Apalis Redis 1.1.1 with custom delivery/idempotency/locks | Confirmed |
| Project license is undeclared | Confirmed |
| Idle poll interval is independent of heartbeat in 1.2.0 | Confirmed |
| Lost Redis lease does not stop running coroutine (cancel requested; ping budget exits) | Confirmed (docs/limitations) |
| Oxana reduces ownership custom code vs Apalis | **Hypothesis** |
| watchexec-supervisor shrinks supervisor LOC without losing process-group semantics | **Hypothesis** |
| Dual-worker fencing + A/B renewal pass on 1.2.0 | **Unproven** (Redis absent here; prior stand was rc7 / single-worker) |

## 8. Stage A deliverables

| Deliverable | Path |
| --- | --- |
| This baseline | `docs/research/baseline.md` |
| Capability matrix | `docs/research/capability-matrix.md` |
| Candidate comparison | `docs/research/candidates.md` |
| Architecture plan (Stage B draft) | `docs/architecture/plan.md` |
| ADR stubs | `docs/adr/0001-*.md` … |

## 9. Explicit non-claims

- No performance numbers from this VM (no Redis, no wheel rebuild for research commit).
- No GREEN/RED behavioral suite executed against Redis in Stage A.
- `1.0.0rc7` long-run passes are not `v1.2.0` correctness proofs.
- Choosing Oxana or watchexec is **not** finalized; only research + spike plan.
