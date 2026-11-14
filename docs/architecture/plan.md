# Architecture plan — Ports & Adapters fork

**Stage:** B (architecture) — contracts and diagrams **before** mass code move.  
**Baseline:** `v1.2.0` (`9164eea…`). Main has moved past that tag. The process that runs is [architecture.md](../architecture.md).  
**Patterns:** Hexagonal / Ports & Adapters ([Cockburn](https://alistair.cockburn.us/hexagonal-architecture)); Tokio discover→notify→wait ([shutdown](https://tokio.rs/tokio/topics/shutdown)); AAA tests.

This plan does **not** rewrite `worker.rs` wholesale. Stage C has not started. `PARENT_NO_CPYTHON_IMPLEMENTED` is `false`.

---

## 1. Target crate layout

What main has, and what this plan still wants:

```text
Landed, and running:
  crates/simple-q-runtime/   # Redis, locks, admin, extension
  crates/agent-worker/       # supervisor.rs and the Python child

Landed, not linked:
  crates/simple-q-core/      # Attempt, drain, parent bootstrap, port types
                             # publish = false; no Redis, PyO3, or OS types

Target, not created:
  crates/runtime/            # orchestration, attempt lifecycle, drain barrier
  crates/supervisor/         # process adapter + local policy
  crates/backend/            # Apalis QueuePort; Oxana only if a later spike wins
  crates/python-bridge/      # PyO3 child-only; BridgeOutcome mapping
  crates/cli/                # composition root

python/                      # public API, unchanged surface
tests/ contract/ integration/ process/ e2e/   # not split yet
benchmarks/                  # not added
```

**Composition root:** the target `crates/cli` wires adapters. `simple-q-core` never constructs Redis clients or `pyo3::Python`. The binary that ships still wires Redis and Python inside `agent-worker`.

---

## 2. Responsibility boundaries

| Actor | Owns | Must not |
| --- | --- | --- |
| Rust parent | spawn/wait/reap, signals, escalation, restart/crash-loop, memory/recycle, process telemetry | init CPython; ACK/retry; delivery decisions |
| Queue runtime (child) | claim, ownership renew/recovery, ACK/retry, delivery accounting | app Postgres; app FSM transitions |
| Python child | handlers, DI, hooks, asyncio, workflow/FSM/checkpoints, app idempotency | process supervision; Redis claim implementation |

Delivery: **at-least-once**. No exactly-once promise.

---

## 3. Ports (binding)

Full text: [`contracts.md`](contracts.md). Summary:

- **Fetch loop:** owned by the queue **adapter** (Apalis `WorkerBuilder`). Core receives `Attempt`; no core `claim()` poll loop.
- **Finish:** `finish(attempt_id, ownership, disposition) -> FinishDisposition` (includes stale reject and unconfirmed).
- **Lease renewal:** backend-driven; loss via `AttemptEvent::LoseOwnership` / `BridgeOutcome::LeaseLost`.
- **AdminPort** segregated from finish.
- **ProcessPort:** spawn/signal/wait; candidate `watchexec-supervisor` **6.0.0**.
- **PythonBridgePort** (child only): closed `BridgeOutcome`; cancel request ≠ confirmed stop.

**Non-goals:** universal multi-backend queue; plugin framework; trait-per-type.

---

## 4. State machines

### 4.1 Process (parent slot)

```text
Starting → Running → Draining → Stopping → Exited
                  ↘ Failed
```

| Item | Spec |
| --- | --- |
| Trigger | start policy / SIGTERM / memory soft / hard / crash |
| Owner | supervisor policy + ProcessPort adapter |
| Effects | spawn, SIGUSR1, SIGKILL, reap, roster publish |
| Forbidden | Running→Exited without wait/reap; reset slot while old child not Exited |
| Idempotent stop | second drain while Draining is no-op or reinforce signal per policy (document) |
| Concurrent | stop vs recycle vs memory kill — single transition owner per slot |

### 4.2 Attempt (child execution)

See `contracts.md` and `AttemptLifecycle` (execution × ownership × finish).  
`HandlerReturned` must not clear `ownership = Lost`. `ExecutionState::Stopped` means stop already confirmed.

### 4.3 Drain (child + parent)

Ordered (`DrainPhase` + `AdmissionGate`):

1. Close **admission**  
2. Settle in-flight claims; register accepted Attempts; `mark_inflight_settled`  
3. `TaskTracker::close` (allows empty wait — does **not** block new registration by itself)  
4. `tracker.wait`  
5. Hooks → child exit → parent reap → escalate  

Empty Python task list ≠ drain complete.

---

## 5. Pattern records

Format: `проблема → решение → место → альтернатива → цена → тест`.

### Ports & Adapters

```text
ядро смешано с Redis/PyO3/OS
→ порты + адаптеры; composition root в cli
→ crates/simple-q-core + future adapter crates
→ «новая папка поверх монолита» без портов
→ стоимость: перенос по capability + contract tests
→ unit на fake QueuePort; contract suite на Redis adapter
```

### Explicit state machines

```text
неявные флаги draining/shutdown/roster
→ enum + допустимые переходы
→ core::lifecycle
→ ad-hoc AtomicBool only
→ цена: спецификация + матрица переходов
→ unit given_when_then на каждый запрещённый переход
```

### Supervision mechanism vs policy

```text
supervisor.rs = spawn+policy+redis+memory
→ watchexec-supervisor mechanism; policy crate local
→ crates/supervisor
→ оставить custom process
→ цена: spike + process tests
→ process: SIGTERM drain, crash-loop, group cleanup
```

### Shutdown barrier

```text
cancel fetch может сорвать ACK; нет полного учёта задач
→ notify shutdown ≠ wait; TaskTracker
→ crates/runtime
→ только watch channel
→ цена: регистрация каждой long task
→ unit: fetch cancelled, ACK still runs
```

### Lease / fencing

```text
смешение timeout/lease/heartbeat/lock/dedup
→ раздельные механизмы; owner-checked renew/release
→ backend adapter + lock adapter; docs where fencing checked
→ один TTL на всё
→ цена: диагностика + A/B режим без renewal
→ contract: stale ACK rejected; dual worker claim
```

### Typed bridge

```text
outcome — свободная строка JSON
→ закрытый BridgeOutcome; unknown = protocol error
→ python-bridge
→ stringly match в worker
→ цена: версия wire + тесты
→ unit mapping + integration unknown outcome
```

---

## 6. Parent without CPython

Binding: [`adr/0004-parent-without-cpython.md`](../adr/0004-parent-without-cpython.md).

Parent: CLI + `REDIS_URL` + `SIMPLE_Q_*`. Child: `App.client` unchanged. Mismatch logged via effective Redis host/db. One-shot export child deferred.

---

## 7. Stage roadmap (execution order)

| Stage | Exit criterion |
| --- | --- |
| **A** | baseline + matrix + candidates (this research) |
| **B** | this plan + ADRs; no mass move |
| **C** | vertical slice: wheel → parent(no Py) → child → handler → finish → drain → reap; process+integration green |
| **D** | ownership loss, SIGKILL, Redis outage, concurrent lifecycle |
| **E** | migrate one capability at a time; delete replaced code |
| **F** | matrix before/after, CI split, benchmarks, known limits |

Agent TDD on every capability: contract → RED → minimal GREEN → refactor.

---

## 8. Testing architecture (preview)

See `docs/testing/strategy.md`.

- AAA mandatory; names `given_*_when_*_then_*`.  
- Unit: no Redis/processes.  
- Contract: same suite for comparable backends.  
- Integration: real Redis; unavailable Redis = **failure**, not skip.  
- Process: real signals/reap.  
- E2E: installed wheel, not src import.  
- Diagnostic `renewal_enabled=false` test-only mode (ADR before promoting).

---

## 9. CI split (target)

1. format / lint / types  
2. unit (no Redis)  
3. contract+integration (**must** Redis)  
4. process  
5. wheel + isolated install  
6. RC long/fault suite  

Artifacts per run: `run_id`, SHA, dirty, versions, effective config, seed, OS/arch, limits, journals, logs, Redis observations, exits, failure reason. Statuses: `passed|failed|incomplete|skipped|environment_error`.

---

## 10. What we will not do in early stages

- Mechanical rewrite of `worker.rs` / `supervisor.rs` without ports and RED tests.  
- Add daemon, plugin system, event bus, CQRS, or “universal queue”.  
- Treat Oxana migration as decided.  
- Claim bench results from `1.0.0rc7` as `1.2.0` proofs.
