# Changelog

## Unreleased

An active `peek` page is a list offset. A missing id does not fail the next page. A cursor that names a task id raises `ValueError`. Stored `error` is at most 4096 bytes.

## 1.0.0rc5

`QueueAdmin.dead` pages dead letters newest-first. A dead record keeps `reason`, `error`, `dead_at`, and `deliveries`. A `dead` result from `dispatch` includes `error`: the exception text for `retries`, `timeout`, and a raised `dispatch`.

`QueueAdmin.peek` pages `active` and `scheduled`. A delivery writes `started_at` once; `locked_at` still moves on each lease renew. A snapshot lists held tasks for every queue on the supervisor. `format_bytes` prints 351112 KiB as `342.9M`.

`{queue}:done` and `{queue}:dead` are sorted sets when Apalis creates the key, and stay sets when the key already is one. Queue counters and the sweeper read both.

Dead tasks written before `simple_q:dead:{queue}` existed are not in that index. `dead` does not list them. They remain in `{queue}:dead` and in the queue counter until the sweeper deletes them, within seven days by default.

The toolchain is Rust 1.99.1. Apalis and apalis-redis are 1.1.1.

## 1.0.0rc4

The worker wheel build compiles inside the manylinux container's own cargo target directory. A host `target/` from Ubuntu 24.04 was being executed there, and its build scripts need glibc 2.34.

A missed Python ping no longer stops business-lock renewal. Renewal stops when the ping budget elapses. A Redis error while extending the lock is retried. Cancelling `dispatch` during unlock still deletes the key. `lock_ttl` must be greater than zero.

## 1.0.0rc3

The worker wheel is `manylinux_2_28`. `agent-worker` in a virtualenv is a launcher that points `LD_LIBRARY_PATH` at the base interpreter's `lib` before `agent-worker.bin`.

`SIMPLE_Q_APP_FACTORY` turns `--factory` on only for `1`, `true`, `yes`, and `on`.

`from simple_q import QueueAdmin` loads the extension. `import simple_q` still does not.

`MemoryClient.drain` calls `startup` and `shutdown`, and puts a `retry` outcome back on the queue. `worker_alive` is false until `set_worker_alive`. A time limit still arrives in the handler as `CancelledError`.

## 1.0.0rc2

The supervisor writes its own Redis record. `QueueAdmin.snapshot` and `agent-worker info` read supervisors, processes, and the tasks those processes hold. They read counters, heartbeats, and inflight tasks.

## 1.0.0rc1

`Client`, `App`, `Config`, and `@actor` replace `configure`, `Registry`, and the memory globals. `on_dead` takes a `DeadLetter`. `worker_alive` is async on the client.
