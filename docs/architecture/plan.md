# Architecture plan — Ports & Adapters fork

**Stage:** B (architecture) — contracts and diagrams **before** mass code move.  
**Baseline:** `v1.2.0` (`9164eea…`).  
**Patterns:** Hexagonal / Ports & Adapters ([Cockburn](https://alistair.cockburn.us/hexagonal-architecture)); Tokio discover→notify→wait ([shutdown](https://tokio.rs/tokio/topics/shutdown)); AAA tests.

This plan does **not** rewrite `worker.rs` wholesale. Stage C is a vertical slice with tests first (Agent TDD).

---

## 1. Target crate layout

Initial structure (ADR may adjust):

```text
crates/
  core/           # types, policy, state machines, ports (no Redis/PyO3/OS types)
  runtime/        # orchestration, attempt lifecycle, drain barrier
  supervisor/     # process adapter (watchexec*) + policy (slots, memory, crash-loop)
  backend/        # Apalis (default) QueuePort adapter; Oxana only if spike wins
  python-bridge/  # PyO3 child-only; BridgeOutcome mapping
  cli/            # composition root, commands, wheel bin entry

python/           # public API, DI, hooks (unchanged surface as much as possible)
python/tests/unit/

tests/
  contract/
  integration/
  process/
  e2e/
  support/

benchmarks/
  pickup/ throughput/ longrun/

docs/
  architecture/ adr/ research/ testing/
```

**Composition root:** `crates/cli` wires adapters. Core never constructs Redis clients or `pyo3::Python`.

---

## 2. Responsibility boundaries

| Actor | Owns | Must not |
| --- | --- | --- |
| Rust parent | spawn/wait/reap, signals, escalation, restart/crash-loop, memory/recycle, process telemetry | init CPython; ACK/retry; delivery decisions |
| Queue runtime (child) | claim, ownership renew/recovery, ACK/retry, delivery accounting | app Postgres; app FSM transitions |
| Python child | handlers, DI, hooks, asyncio, workflow/FSM/checkpoints, app idempotency | process supervision; Redis claim implementation |

Delivery: **at-least-once**. No exactly-once promise.

---

## 3. Ports (minimal)

Interfaces from **needed contracts**, not library mirroring.

```text
QueuePort
  enqueue(job, delay?)
  claim / start worker loop hooks (or stream of Attempt)
  renew / observe lease (or backend-driven with loss signal)
  ack(attempt)
  retry(attempt, delay)
  dead(attempt, reason)
  admin reads used by snapshot (segregated AdminPort if consumers differ)

ProcessPort
  spawn(spec) -> ChildId
  signal(ChildId, Signal)
  wait(ChildId) -> ExitStatus
  # process-group semantics explicit in adapter

PythonBridgePort  (child only)
  load_app / dispatch(job) -> BridgeOutcome
  request_cancel(execution_id)
  hooks startup/shutdown

ConfigPort / ClockPort / MetricsPort
  as needed for tests (fake clock in unit)
```

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

```text
Claimed → HandlerRunning → HandlerFinished → Finishing(Ack|Retry|Dead) → Terminal
                ↘ LeaseLost / Cancelled / BridgeFailed
```

Handler return ≠ ACK complete. Finishing work registered in TaskTracker; fetch cancel does not cancel Finishing.

### 4.3 Drain (child + parent)

Ordered:

1. Close admission  
2. Race with in-flight claim — explicit contract  
3. Wait accepted work  
4. Wait result/ACK/retry ops  
5. Cleanup + shutdown hooks  
6. Exit child  
7. Parent wait/reap  
8. Escalate on separate budgets  

Empty Python task list ≠ drain complete.

---

## 5. Pattern records

Format: `проблема → решение → место → альтернатива → цена → тест`.

### Ports & Adapters

```text
ядро смешано с Redis/PyO3/OS
→ порты + адаптеры; composition root в cli
→ crates/core + crates/*-adapter
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

**Problem:** parent calls `runtime::initialize()` + `bridge::read_client` to learn Redis URL/`Config`.

**Plan:**

1. Prefer `REDIS_URL` + `SIMPLE_Q_*` already used by enqueue/admin for parent composition.  
2. Optional: write resolved effective config from a **one-shot child** or installer — only if product requires App object fields not expressible in env (ADR).  
3. Positive process test: parent binary path never links/initializes interpreter (or feature-gate PyO3 out of parent target).

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
