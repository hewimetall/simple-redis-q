# Testing strategy — AAA, levels, and Stage gates

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
python -m pytest python/tests/test_runtime_contract.py -q  # until split to python/tests/unit

# integration (Redis required)
redis-cli ping
cargo test -p simple-q-runtime --test admin --test poll_interval -- --test-threads=1
```

Record command + exit status in stage reports (brief §15.7).
