# Architecture

These pages describe simple-q 1.0.

The containers inside simple-q are **Client**, **App**, **Config**, the **worker supervisor**, and **Redis**. Client sends. App runs the handler and the dead-letter hook. `Config.from_env` reads only `SIMPLE_Q_*`. The worker uses the Redis URL stored on the `App`'s `Client`. `agent-worker enqueue` and `agent-worker admin` read `REDIS_URL` themselves. `agent-worker info` uses the same client URL as `worker`. `SIMPLE_Q_APP` and `SIMPLE_Q_APP_FACTORY` are read by `worker` and `info`, not by `Config.from_env`. `HOSTNAME`, `BOARD_*`, `QUEUE`, `RUST_LOG`, and `DRAMATIQ_PROM_*` are read by the worker name, the board, the log filter, and the metrics process. The worker supervisor forks children, watches memory, and publishes its own Redis record. It does not take messages and it does not heartbeat as a queue worker. Redis holds the queue and that supervisor record.

This is the context diagram for that model.

```mermaid
C4Context
  title System Context
  System_Ext(redis, "Redis", "Queue, supervisor records, heartbeats, idempotency, and inflight messages.")
  System_Ext(cgroup, "cgroup", "Container memory.current, memory.stat, and memory.max.")
  System_Ext(exporter, "Python exporter", "OpenTelemetry exporter supplied by the application.")
  Person(producer, "Application", "Sends a task and reads a worker heartbeat.")
  Person(operator, "Operator", "Reads agent-worker info or QueueAdmin.snapshot.")
  System(simple_q, "simple-q", "Queue runtime for Python async tasks.")
  Rel(producer, simple_q, "Sends tasks and reads a worker heartbeat")
  Rel(operator, simple_q, "Reads supervisors, processes, and held tasks")
  Rel(simple_q, redis, "Enqueues, heartbeats, supervisor records, and acks")
  Rel(simple_q, cgroup, "Reads memory.current and memory.stat")
  Rel(simple_q, exporter, "Exports finished spans on one OS thread")
  UpdateLayoutConfig($c4ShapeInRow="2", $c4BoundaryInRow="1")
```

## Call chains

These are the paths the code actually takes. Each chain stops at a Redis command or at the Python handler.

### Send and a delayed retry

`Client.send` and `Client.send_with_options` both call `prepare`, then `enqueue_prepared`, then `enqueue_job`. `prepare` runs `on_send` on the caller's task and builds a job with `retries = 0`. A first send with `unique_key` goes through `enqueue_unique`: apalis `SETNX`s `{queue}:idempotency:{key}` and sets its TTL to `SIMPLE_Q_UNIQUE_TTL_SECS`. A held key returns `None` from `send_with_options`. `send` raises in that case.

A retry does not start at `send`. After `App.dispatch` returns `retry`, `handle_job` adds one to `retries` and calls `enqueue_job` with the delay. `extend_idempotency_for_delay` then `EXPIRE`s the same key for the wait plus `unique_ttl`. The retry does not claim the key again.

### One delivery

The apalis worker calls `handle`, which calls `handle_job`. `incr_delivery` runs `INCR` and `EXPIRE` on `delivery:{message_id}:{retries}` before Python. `decide` returns `Run` when that count is at or below `max_deliveries`. `bridge.execute` calls `App.dispatch`. The handler is `declared.function` under `asyncio.wait_for`. Success deletes the delivery key and the idempotency key. A failure before the last attempt returns `retry` and keeps the idempotency key. The last failure calls `on_dead` and leaves the delivery key.

### Poison, before the handler

`decide` returns `Poison` when the `INCR` result is greater than `max_deliveries`. The count that equals the limit still runs. `handle_job` calls `App.note_dead` with reason `poison` and `error` unset. It does not call `dispatch`. The idempotency key is deleted. The delivery key stays, so the next start of that attempt is still poison.

`on_dead` receives one `DeadLetter`. `error` is `"{Type}: {message}"` only for `retries` and `timeout`.

### Worker heartbeat

`Client.worker_alive` uses the connection opened by a previous send. A client that has never connected returns `False` and does not call Redis. Otherwise the runtime parses `agent-worker.{queue}.{host}.{pid}.{suffix}`, reads `ZSCORE {queue}:workers`, and compares the score with Redis `TIME` against `heartbeat_secs * missed_heartbeats`.

### In-memory drain

`MemoryClient.drain` pops queued messages and calls `App.dispatch`. A `retry` outcome leaves the `unique_key` held and does not put the message back. Any other outcome, and a raised handler, release the key.

### Memory pressure

Once a second the supervisor reads each child's RSS and, when `memory.max` is finite, the cgroup working set (`memory.current` minus `inactive_file`). A per-child byte limit drains with `SIGUSR1` or kills with `SIGKILL`. A ratio `Kill` signals the largest child only when that child's RSS is at least half the usage. Otherwise the largest not-yet-drained child gets `SIGUSR1`. The child treats `SIGUSR1` as stop-taking-work. `SIGKILL` has no handler. A signal is stored on the child row only after `signal_child` succeeds. A second `SIGUSR1` does not replace the first. Anything after `SIGKILL` is ignored, and the kill is visible before `wait`.

### Supervisor record

`bridge::read_client` loads the `App` and returns the Redis URL and `Config` from its `Client`. The supervisor opens its own connection with that URL. Every `SIMPLE_Q_HEARTBEAT_SECS` it calls `publish_supervisor`. A failed connect is logged and retried on the next tick. Children still start.

`publish_supervisor` reads Redis `TIME`, then `HSET`s `simple_q:supervisor:{host}.{pid}` and `EXPIRE`s it for two heartbeat windows. The member `{host}.{pid}` is `ZADD`ed to `simple_q:supervisors` with that same Redis time as the score. Host comes from `HOSTNAME` or `/etc/hostname`. The hash stores the start fields (`host`, `pid`, `started_at`, `version`, `queues`, `processes`, `concurrency`, `max_tasks`, `child_soft`, `child_hard`), the latest cgroup working set, and the counters `recycled`, `drains`, `kills`, and `restarts`. Each child still in the roster is `child:{pid}` JSON: `slot`, `state`, `reason`, `rss`, `started_at`, `ended_at`, `exit`, `tasks_done`, and `max_tasks`. `started_at` for the supervisor, and for a child that has not been stamped yet, is Redis `TIME` from this publish. An exit before the first successful publish is stamped with that same Redis time. A finished child row is `HDEL`ed after two windows. The hash itself is not deleted. Members older than one window are removed with `ZREMRANGEBYSCORE`.

On `Event::Start` the child calls `note_worker_start`. That `HSET`s `ppid`, `started_at`, `max_tasks`, and `slot` when `SIMPLE_Q_SLOT` is set, on `{queue}:workers:metadata:{name}`. Apalis owns that hash and does not wipe these fields. Every heartbeat the child `HSET`s `tasks_done` on the same hash and `tasks:{pid}` on the supervisor hash. An orderly exit stops that writer, writes the final count, then deletes the metadata hash. The next `publish_supervisor` copies `tasks:{pid}` into the child JSON and fills `max_tasks` from the supervisor record, so the exited row keeps both.

`wait` stores the exit code, or the signal number when the child was signaled. How `state` and `reason` are read is on the [tracing and admin](operations.md) page. `recycled` counts each exit 0. `drains` counts `SIGUSR1` for `memory_soft` and `memory_ratio`, not shutdown. `kills` counts a supervisor `SIGKILL` for the per-process hard limit and for the hard cgroup ratio. `restarts` counts each killed child, including `ping_budget`, `sigkill`, and `crash`, and does not count the memory kill a second time.

### Snapshot

`agent-worker info` calls `bridge::read_client`, connects, then `admin::snapshot`. It does not register a worker and it does not write a heartbeat. `QueueAdmin.snapshot` calls the same function on the caller's `Client` connection and loop. `from simple_q import QueueAdmin` loads the extension. `import simple_q` does not. `--json` and the Python object are the same value.

`snapshot` does not walk the waiting, done, or dead lists. It reads three batches. The first is Redis `TIME`, `ZRANGEBYSCORE` on `simple_q:supervisors`, and, per queue, `LLEN` of the active list, `ZCARD` of the scheduled set, `SCARD` of done, `SCARD` of dead, and `ZRANGE` of workers with scores. The second is `HGETALL` for each supervisor still inside the heartbeat window, then `HGETALL` of the metadata hash and `SMEMBERS` of the inflight set for each worker. A queue's inflight count is the sum of those sets for every worker of the queue. A `host` filter limits supervisors, their children, and orphans. It does not limit the queue counters. The third batch, per held task, is `HGET` of the payload, `HMGET` of `locked_at`, then `GET` of `delivery:{message_id}:{retries}`. A missing payload still yields the task id. Queue key names come from the apalis config for that queue.

A process is `(host, pid)`. The worker name is `agent-worker.{queue}.{host}.{pid}.{suffix}`. The pid is the numeric field before the suffix, so a host may contain dots. The process attaches to the supervisor `(host, ppid)` from the metadata hash. A supervisor child row stays after that hash is deleted. `tasks_done` is the max across the process's worker hashes and the child JSON. Summing those hashes would count one process once per queue. `max_tasks` is the max as well. When the child row still says running and `tasks_done` has reached `max_tasks`, the snapshot shows `draining` / `max_tasks`. An orphan whose metadata hash is still present and whose heartbeat is stale is `killed` with no reason. An orphan with no hash is `exited` with no reason. A fresh heartbeat with no live supervisor stays `running`.

### Tracing

`tracing=None` installs no OpenTelemetry layer. Otherwise `PythonBatchProcessor.on_end` queues the finished span, and the OS thread `otel-export` calls `exporter.export`. Tokio tasks do not call into Python. Exit 2 skips the flush.
