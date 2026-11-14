# Spike results (reproducible)

Research VM after Redis install. Toolchain: `rustup run 1.99.1`.

## Environment proof (v1.2.0 @ `9164eea`)

| Check | Command | Result |
| --- | --- | --- |
| Redis | `redis-server` 7.0.15; `redis-cli ping` | `PONG` |
| Rust integration | `cargo test -p simple-q-runtime --test admin --test poll_interval -- --test-threads=1` | **20 passed** |
| Python integration | `pytest python/tests/test_orphan_integration.py test_worker_info.py test_queue_admin.py` | **8 passed** |

Previous Stage A claim “no Redis / integration unproven” is **superseded for this VM**.

## watchexec-supervisor **6.0.0** (pinned)

```text
question: Can Job express start → SIGUSR1 → stop → wait/reap?
hypothesis: yes via start_job + Signal::User1 + stop + to_wait + delete_now
experiment: docs/research/spikes/watchexec-6.0.0
criterion: compile + run prints ok
```

```bash
cd docs/research/spikes/watchexec-6.0.0
rustup run 1.99.1 cargo run
# spike-watchexec-job: ok watchexec-supervisor=6.0.0
```

**Confirmed API only:** `start_job`, `job.start/signal/stop/to_wait/delete_now`, `Signal::User1`, `Program::Exec`.  
**Not proven:** graceful drain equivalent to agent-worker (child that handles SIGUSR1, READY handshake, exit-status assert). `/bin/sleep` ignores USR1; spike fails hard on wait timeout.  
**Checksum (crates.io 6.0.0):** `f1da1c4eb0277893e2ab8c888db407d979e31c9ce94c9d3664c49789691a3441`  
**Decision lean:** adopt as ProcessPort mechanism after a real drain child-fixture + process-group tests (Stage C/D).

## oxana **2.5.0** (pinned)

```text
question: Can one Worker handle opaque JSON (all Python actors) without per-actor types?
hypothesis: OpaqueJob { message: Value } + one Worker compiles
experiment: docs/research/spikes/oxana-2.5.0
criterion: cargo build succeeds
```

```bash
cd docs/research/spikes/oxana-2.5.0
rustup run 1.99.1 cargo build
# compile ok oxana=2.5.0 OpaqueJob={message: Value}
```

**Confirmed:** one opaque job type is enough at the type level (disproves “must wrap every actor”). Needs `async-trait` on the consumer because `#[derive(oxana::Worker)]` expands to it.  
**Also confirmed from oxana-2.5.0 sources:** claim lives in `dispatcher` (`claim_job` / LMOVE); heartbeats gate resurrection (`resurrect_claim.lua`). Using Oxana only as dumb storage while owning finish outside its executor fights that design.  
**Decision:** keep Apalis 1.1.1 behind QueuePort; Oxana remains non-default unless a full-runtime spike (Oxana owns claim+finish, Worker only calls Python bridge) beats Apalis on ownership tests.

## Upstream chain template (filled for incumbent)

```text
apalis-redis lock_tasks / heartbeat
→ used as-is in simple-q v1.2.0
→ crate apalis-redis 1.1.1 checksum 39338ae8ab10d9c9389eb2f8b53ad9698750f8cf9802404d738de139fc359380
→ regression: crates/simple-q-runtime/tests/poll_interval.rs + orphan_integration (ran green on this VM)
```

No new upstream patch required for Stage B.
