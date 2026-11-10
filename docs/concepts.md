# Concepts

These pages describe simple-q 1.0.

A send stores one version-1 message. `message_id` stays the same across retries. `retries` counts handler failures. `deliveries` counts how many times a worker has started that attempt. Those two numbers are independent.

## Heartbeat

Each child writes its worker name into the sorted set `{queue}:workers`. The score is Redis `TIME`. The default interval is 15 seconds (`SIMPLE_Q_HEARTBEAT_SECS`). A worker is alive while that score is inside `heartbeat * missed` seconds. The default missed count is 4 (`SIMPLE_Q_MISSED_HEARTBEATS`), so the default window is 60 seconds.

`await client.worker_alive(name)` reads that score. The caller's `Config` must use the same heartbeat and missed-heartbeat values as the workers. The clock is Redis `TIME`, so the caller's clock does not matter. A name that does not parse, a missing score, or a client that never opened Redis is not alive.

The worker name has the shape `agent-worker.{queue}.{hostname}.{pid}.{suffix}`. The queue is the second field and is one lowercase ASCII word. `hostname` comes from `HOSTNAME` or `/etc/hostname`.

While any worker for the queue is inside the window, its heartbeat asks Redis to put abandoned inflight messages back on the queue. That requeue does not increment the delivery counter. The next time a worker starts the attempt, it does. If no worker for the queue is heartbeating, inflight messages stay where they are.

A child pings the Python loop every 2 seconds with a 3 second timeout. `PingWatch` exits the child only after the loop has stayed silent for the whole ping budget. The budget is the actor time limit plus 60 seconds unless `SIMPLE_Q_PING_BUDGET_SECS` sets it. The default time limit is 30 minutes, so the default budget is 1860 seconds. One minute of silence does not exhaust that budget. A successful ping clears the silence clock. When the budget elapses, the child stops renewing business locks, asks the running tasks to cancel, and exits 2. Inflight messages are left for another worker. Exit 2 does not flush spans.

The child ignores `SIGTERM`. The supervisor owns process recycling.

## Poison

`decide` compares the delivery count with `max_deliveries`. The count is the value Redis returned from `INCR`, so the first run of an attempt is 1. A count **above** `max_deliveries` is poison. The delivery that lands **on** the limit still runs.

With the default `max_deliveries` of 5, deliveries 1 through 5 run the handler. Delivery 6 is poison. `decide(5, 5)` runs. `decide(6, 5)` is poison. The same rule holds for a limit of 1: the first delivery runs, and the second is poison.

The worker calls `decide` before it enters Python. A poison message does not call the handler. The dead-letter hook runs with reason `poison` and `error` unset. The delivery key stays, so a later start of the same attempt still sees a count above the limit. The queue itself is not rewritten to hide the message.

Orphan redelivery and a process crash share this counter. The requeue script does not increment it. The worker does, at the start of the attempt.

## Process restart after N tasks

`--max-tasks-per-process` defaults to 500. Each finished delivery counts, including a poison delivery that never entered the handler. When the count reaches the limit, the child stops taking new work and drains the handlers already running. It then exits. The supervisor starts a replacement child in that slot.

Exit 0 is a normal recycle. Exit 1 and exit 2 restart the child. A child that disappears without a status, including `SIGKILL`, restarts. Exit 3 is configuration: a bad memory limit or a bad tracing setup. The supervisor stops instead of looping on it.

Restarting a child does not change poison. A delivery that already passed `max_deliveries` is buried on the next start, before the handler.

## Memory

The supervisor checks memory about once a second. It does not take messages and it does not call Redis.

### Per-child RSS

`--child-memory-soft` and `--child-memory-hard`, or `SIMPLE_Q_CHILD_MEMORY_SOFT` and `SIMPLE_Q_CHILD_MEMORY_HARD`, set an RSS limit for one child. The flag wins when both the flag and the variable are set. `K`, `M`, and `G` are powers of 1024. An optional `B` is the same unit. A bare number is bytes. Zero, a fraction, and any other shape are invalid and the supervisor exits 3 before it starts a child. When both limits are set, soft must be below hard.

RSS comes from `/proc/<pid>/statm`. At the soft limit that pid receives `SIGUSR1` once and drains: it finishes current work and takes nothing new. At the hard limit that pid receives `SIGKILL`. A later child is a new process. The queue and the poison rule stay as they were.

If `processes * hard` is greater than a finite cgroup `memory.max`, the supervisor logs a warning and keeps running.

### Cgroup ratio

The ratio backstop uses the cgroup working set: `memory.current` minus `inactive_file` from `memory.stat`. That is the same subtraction kubelet uses. When `inactive_file` is absent, the usage is `memory.current`. `working_set` is that subtraction.

`SIMPLE_Q_MEMORY_SOFT_RATIO` defaults to 0.80. `SIMPLE_Q_MEMORY_HARD_RATIO` defaults to 0.95. Both must be finite numbers in the open interval `(0, 1)`. At the soft ratio the supervisor sends `SIGUSR1` to the child with the largest RSS that has not already been asked to drain. The next tick picks another child if usage is still high. At the hard ratio the supervisor sends `SIGKILL` to the largest child when that child's RSS is at least half the working set (`rss * 2 >= usage`). A child that does not own the usage is drained instead of killed, because killing it would not stop the cgroup from running out of memory.

Without a finite `memory.max` (`max`, empty, or missing), the ratio check does nothing. The per-child byte limits still apply.

The supervisor writes `simple_q_child_memory_drains_total` and `simple_q_child_memory_kills_total` into `simple_q_child_memory.prom` in the metrics directory. The scrape adds those samples. A missing file is zero for both, and the scrape still returns 200.

How this interacts with kernel OOM is on the [deploy](deploy.md) page. A cgroup with `memory.oom.group` kills every process in the container, so the supervisor has to `SIGKILL` the child itself before that happens.
