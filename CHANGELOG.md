# Changelog

## Unreleased

A soft memory signal skips a child that is inside a delivery. The supervisor keeps one anonymous memfd for its own lifetime, one `AtomicU32` per slot. The child maps that page after start, closes the descriptor, and counts each delivery. The parent stores 0 before it starts a child in that slot, so a digit left by `SIGKILL` does not mark the next child busy. A hard signal does not read the digit. On macOS there is no memfd, and a soft signal uses RSS only.

## 1.1.0

The supervisor chooses a child for a memory limit by RSS. It does not look at whether that child is running a task. A per-child soft limit sends one `SIGUSR1` to every child over its own threshold, and a per-child hard limit sends `SIGKILL`. Several children can be signaled in the same second. At 80% of the cgroup working set, one not-yet-drained child, the largest RSS, gets `SIGUSR1`. If usage is still high a second later, the next largest gets it. At 95%, one child, the largest, is `SIGKILL`ed when its RSS is at least half the working set. If that child is already being killed for its own hard limit, no second child is killed. When the largest is below half, nobody is killed for the ratio, and the largest child that has not been asked to drain gets `SIGUSR1`. A `SIGUSR1` that the kernel does not deliver is not recorded, so the next second tries that child again.

Lock-key examples are `demo:gate` and `demo:item:{name}`.

The worker keeps trying to delete `unique_key` until the task is in `done` or `dead`, the process stops, or `SIMPLE_Q_UNIQUE_TTL_SECS` elapses. A Redis error during that wait is retried. A slow ack no longer leaves the key for the rest of the TTL while the process is still up. The default TTL stays 86400 seconds.

CI keeps a separate Rust cache for each Python version. `PYO3_PYTHON` is the interpreter path from the job, so `agent-worker` links that version's `libpython`.

## 1.0.0rc7

Wheels are built for CPython 3.12, 3.13, 3.14, and 3.15, each with the GIL. Free-threaded builds are not produced. `simple-q-runtime` is one wheel per minor version. `simple-q-worker` links `libpython3.X.so.1.0` for that same minor. The launcher reads `pyvenv.cfg` `version` and prepends the directory that holds that `libpython`.

On macOS, per-child RSS comes from `proc_pid_rusage` (`ri_resident_size`). The cgroup ratio still reads `/proc/self/cgroup` and stays off. Published wheels stay Linux.

The supervisor installs `SIGTERM` and `SIGINT` before it imports the application, so a signal during that import still stops the process before any child starts. An orderly stop deletes the supervisor hash and its sorted-set member. The idempotency key is deleted after the task is in `done` or `dead`. The child waits for that delete before it exits.

## 1.0.0rc6

An active `peek` page is a list offset. A missing id does not fail the next page. A cursor that names a task id raises `ValueError`. Stored `error` is at most 4096 bytes.

The arm wheel build installs the pinned compiler without rustfmt. The 1.0.0rc5 tag's arm job stopped on a `cargo-fmt` proxy conflict and did not publish.

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
