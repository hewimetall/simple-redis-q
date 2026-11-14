# Capability matrix — `v1.2.0` → architectural fork

Format:

```text
возможность → текущая реализация → кандидат на замену
→ покрытие → пробел → оставшийся собственный код
```

Coverage labels: `unit` / `contract` / `integration` / `process` / `e2e` / `none` / `partial`.  
Baseline code: tag `v1.2.0` (`9164eeab0361639d8cd2d0ca2bb91c355710c1a8`).

---

## Queue runtime

### Atomic claim / ownership

```text
atomic claim + lease ownership
→ apalis-redis RedisStorage + lock_tasks(true); meta locked_at renew
→ keep Apalis (default) | Oxana storage spike
→ partial (orphan_integration, admin snapshot; no dual-worker fencing suite)
→ no A/B renewal-off; no typed lease-loss outcome to Python
→ delivery counter, poison decide(), unique_key terminal release, started_at stamps
```

### Pickup latency / empty fetch

```text
idle fetch cadence
→ Apalis poll_with_interval(SIMPLE_Q_POLL_INTERVAL_MS, default 500)
→ keep Apalis interval strategy
→ partial (poll_interval Rust test)
→ no pickup p50/p95/p99 benchmarks checked in this research
→ Config + queue_config wiring
```

### Lease renewal / lease loss

```text
task lease renew vs loss while handler runs
→ Apalis heartbeat/orphan; Python cancel requested; ping budget exits child
→ keep Apalis heartbeats; add typed BridgeOutcome::LeaseLost; diagnostic renewal-off mode
→ partial (docs/limitations; lock renew unit tests for business lock only)
→ task-lease loss ≠ business-lock loss; fencing on stale ACK unproven
→ outcome mapping, cancel bridge, ping watch
```

### ACK / retry / recovery

```text
finish paths + orphan requeue
→ handle_job outcomes + Apalis ack/abort; orphan via missed_heartbeats
→ keep Apalis finish API behind QueuePort
→ partial (runtime_contract, enqueue tests)
→ fault barriers (effect done/ACK lost) missing; ACK cancel-safety vs fetch cancel
→ retry enqueue, unique_key wait-delete, delivery key lifecycle
```

### Delayed jobs

```text
delayed retry / scheduled
→ enqueue_job with delay_ms; Apalis scheduled set; extend idempotency TTL
→ keep Apalis delay
→ partial
→ contract suite for delay boundaries under dual workers
→ backoff policy in Python + delay enqueue in Rust
```

### Dedup vs execution lock

```text
unique_key (enqueue dedup) vs lock_key (business lock)
→ SETNX idempotency key; separate SET NX PX lock:{key} with owner renew
→ keep custom locks; do not fold into queue lease
→ unit/integration for lock release; unique release-when-terminal tests
→ fencing token not exposed for external HTTP effects (documented non-goal)
→ lock.rs + redis_api idempotency helpers
```

### Delivery counters / poison

```text
delivery count independent of retries
→ delivery:{message_id}:{retries} INCR script; decide() Poison/Run
→ keep custom (Apalis max_attempts stays 1)
→ partial (runtime_contract)
→ contract for retry-count vs delivery-count independence under redelivery
→ job::decide + redis_api delivery helpers
```

### Admin state

```text
snapshot supervisors / held tasks / depths
→ simple_q_runtime::admin + QueueAdmin / agent-worker info
→ keep custom admin over Redis keys; board may stay Apalis or shrink
→ integration (admin.rs, test_queue_admin, test_worker_info)
→ large admin.rs monolith; split read models later
→ admin snapshot + supervisor publish
```

### Redis compatibility

```text
Redis 7/8
→ CI services redis:7 and redis:8
→ same for chosen backend
→ integration job declared
→ this research VM has no Redis (environment_error if run here)
→ none beyond adapter
```

### Cancellation safety

```text
fetch cancel must not cancel ACK/retry
→ JoinHandle tracking for releases; stop_meta watch; pool pause/stop
→ Tokio TaskTracker + explicit admission close (Stage B)
→ partial (shutdown_waits_for_idempotency_release unit)
→ no full drain barrier matrix; no TaskTracker yet
→ Shared releases/announces; drain flags
```

---

## Process supervision

### Spawn / wait / reap / signals

```text
child lifecycle mechanism
→ custom tokio::process + signal_child + JoinSet slots (~1860 LOC supervisor.rs)
→ watchexec-supervisor Job (+ process-wrap groups)
→ process tests exist only indirectly via orphan_integration
→ parent CPython init; no watchexec spike yet; SIGSTOP / process-group cleanup coverage thin
→ slot allocation, crash-loop budget, memory policy, supervisor Redis record
```

### Drain / escalation

```text
SIGTERM → SIGUSR1 drain → wait → escalate
→ watch shutdown channel; request_drain(SIGUSR1); memory hard SIGKILL
→ mechanism via watchexec stop/signal; policy stays ours
→ partial
→ documented drain steps 1–8 incomplete as single state machine; repeated drain races
→ DrainState machine + budgets
```

### Memory / recycling

```text
RSS + cgroup ratio + max_tasks recycle
→ memfd busy digit; soft/hard per-child; ratio drain/kill; exit 0 recycle
→ keep policy; optionally drop memfd if proven unnecessary
→ partial (documented; limited automated process tests)
→ shared-memory busy digit: alignment/invalid slot risks called out in brief
→ Pressure + Roster + slot_busy
```

### Telemetry of processes

```text
supervisor Redis hash + child metadata
→ publish_supervisor / note_worker_start / snapshot
→ keep
→ integration
→ coupled to Python App load in parent
→ admin + ids/names
```

---

## Python bridge

### Outcomes

```text
handler result typing
→ JSON dict outcome strings: ok|retry|dead|skipped
→ closed BridgeOutcome enum (+ protocol error on unknown)
→ partial (dispatch + worker match arms)
→ no lease_loss / bridge_failure / cancellation distinct from timeout in wire type
→ bridge.rs mapping
```

### Asyncio / hooks / DI

```text
App.dispatch, around_dispatch, on_start/on_stop, current_message
→ Python simple_q + PyO3 bridge
→ keep Python ownership; Rust only invokes
→ unit (middleware_stack, runtime_contract)
→ block-the-loop scenarios; unknown outcome protocol tests
→ python package
```

### Parent vs child interpreter

```text
who initializes CPython
→ parent AND child (parent for read_client)
→ child only; parent reads REDIS_URL / config port
→ none as positive test today
→ violates target supply contract
→ composition root + config source
```

---

## Packaging / API

```text
one install command with embedded Rust parent
→ two maturin artifacts: simple-q-runtime + simple-q-worker (depends on runtime)
→ keep that layout; product install = pip install simple-q-worker (ADR 0006)
→ CI worker-wheel job
→ platforms only as declared Linux wheels; macOS local-only
→ build scripts / launcher; parent without Py init (ADR 0004)
```

---

## Testing gaps (aggregate)

| Required scenario (brief §9–10) | v1.2.0 status |
| --- | --- |
| Concurrent claim two workers | gap |
| Living owner renewal | partial (Apalis path; no dedicated dual-worker) |
| Lease loss while handler continues | documented limitation; not contract-tested |
| Stale ACK after new claim | gap |
| Retry vs delivery count independence | partial |
| Unique key TTL + owner-checked release | partial (locks yes; unique terminal yes) |
| Delayed + poison | partial |
| Redis outage vs data loss | gap |
| Bridge unknown outcome | gap |
| Handler done, ACK in flight | partial |
| Supervisor SIGSTOP / crash-loop / descendants | gap / thin |
| A/B renewal on/off | gap |
| Fault-injection barriers | gap |
| E2E isolated wheel install correctness suite | wheel build only |
| AAA naming / Arrange-Act-Assert markers | not project-wide standard yet |

---

## Own code to keep vs replace (preview)

| Keep (policy / product) | Replace candidate | Delete after green |
| --- | --- | --- |
| Python API, DI, hooks, FSM/checkpoints (app-level) | — | — |
| Delivery/poison, unique_key terminal release, business lock | — | — |
| Memory / crash-loop / slot policy | process spawn/wait/signal mechanism → watchexec-supervisor | large chunks of `supervisor.rs` mechanism |
| Apalis adapter (if keep) | thin `QueuePort` | direct Apalis types leaking into core |
| Admin snapshot (product) | split modules | monolith only |
| — | Oxana only if spike wins | Apalis adapter + key assumptions |

See also: `docs/research/candidates.md`, `docs/architecture/plan.md`.
