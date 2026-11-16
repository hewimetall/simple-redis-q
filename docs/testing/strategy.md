# Testing strategy — AAA, levels, and Stage gates

The workflow that runs on main is `.github/workflows/ci.yml`: a Python matrix, Redis integration, a worker wheel, and Rust `cargo fmt --check` plus `cargo clippy -D warnings`. Product tests still skip when Redis is down. The rule below is the target for the fork: a required integration job fails when Redis is absent.

Basis: [Microsoft unit testing AAA](https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-practices); project brief §§7–12, 15.

## AAA (mandatory)

Every test:

```text
Arrange: prepare state, dependencies, observers; wait for explicit readiness
Act:     one behavior or one documented fault scenario
Assert:  result, state, and needed negative invariants
```

Rules:

- Mark `Arrange` / `Act` / `Assert` in code (comments or sections).
- Name: `given_<condition>_when_<action>_then_<outcome>` (or equally specific).
- No arbitrary `sleep` as readiness proof; barriers/channels/bounded polls with timeout + last-state diagnostics.
- No retry-of-test to hide flakes; fix the race or the harness.
- Do not reimplement production algorithms to compute expected values.

## Levels

| Level | Proves | Forbids |
| --- | --- | --- |
| Unit | transitions, policy, config, outcome mapping | Redis, real processes |
| Contract | QueuePort semantics | happy-path-only |
| Integration | real Redis + bridge | faking critical backend |
| Process | spawn, signals, wait, escalation, descendants | treating signal send as completion |
| E2E | installed wheel | importing sources instead of package |
| Benchmark | cost/speed | substituting for correctness |

Unavailable Redis in a required integration job → **failure** / `environment_error`, never silent skip.

## Fault-injection barriers (minimum)

```text
claim done, handler not started
effect done, result not written
handler done, ACK not started
ACK sent, reply lost
lease transferred, old attempt still running
```

Must be reproducible (test hooks/barriers), not sleep-guessing.

## A/B renewal diagnostic

Test-only mode: initial claim + recovery + other heartbeats remain; **lease renewal off**. Not a production default without ADR.

Same fixtures both ways: two workers, handler > initial lease, execution timeout > handler, recovery cadence, versions.

Record TTL/deadlines, last renewals, execution IDs, handler overlap, stale ACK outcome.

## Stand remediation (before trusting long-run)

From brief §11 and resurche PR #1 (rc7 context):

- `Popen` via `poll`/`wait`; reap; distinguish leader vs process group.
- `process_exited` only after confirmed exit; preserve return code; zombies ≠ live.
- Per-attempt duration; no glued spans across executions.
- Separate HTTP retry vs logical step retry vs re-applied effect.

## Stage A note

This research VM has **no Redis**. Stage A does not claim integration GREEN. Next implementation iteration must install/start Redis before contract RED/GREEN cycles.

## Local commands (once env ready)

```bash
# unit (no Redis)
cargo test --workspace
python -m pytest python/tests/test_runtime_contract.py python/tests/test_lock_release.py python/tests/test_version_source.py python/tests/test_alter_option.py python/tests/test_middleware_stack.py python/tests/test_worker_launch.py python/tests/test_interfaces.py python/tests/test_typecheck.py python/tests/test_tracking.py python/tests/test_tracing_defaults.py python/tests/test_lazy_exports.py python/tests/test_admin_kwargs.py python/tests/test_dispatch_edges.py python/tests/test_memory_edges.py python/tests/test_exposition_server.py python/tests/test_multiproc_edges.py python/tests/test_slot_identity.py -q --cov=simple_q --cov-fail-under=100

# integration (Redis required)
redis-cli ping
cargo test -p simple-q-runtime --test admin --test poll_interval -- --test-threads=1
```

Record command + exit status in stage reports (brief §15.7).

## Stage B verification log (this agent VM)

| Suite | Result |
| --- | --- |
| Redis 7.0.15 `PING` | ok |
| `v1.2.0` `admin` + `poll_interval` | 20 passed |
| `v1.2.0` orphan / worker_info / queue_admin | 8 passed |
| Research example, outside the workspace | 12 passed, 1 ignored |
| Stage C gate `--ignored` | **RED** (`PARENT_NO_CPYTHON_IMPLEMENTED == false`) |
| watchexec 6.0.0 spike | ran ok |
| oxana 2.5.0 opaque Worker | compile ok |
