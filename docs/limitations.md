# Limitations

These pages describe simple-q on main. The packages are 1.2.0. Changes after that tag are under Unreleased in the changelog.

## No synchronous send

`Client.send` and `Client.send_with_options` are async. A caller needs a running event loop. There is no synchronous send.

## `catch_panic` is off

The worker does not enable Apalis `catch_panic`. A panic takes down the child, and the supervisor restarts it.

`catch_panic` is off because Apalis retries `Failed` on its own. A handler failure that should stay dead is returned as an abort, which the worker records as killed, and Python enqueues the retry itself when `max_retries` allows one. A panic converted into `Failed` would be scheduled again by the backend, past the actor's `max_retries`.

## A long Redis gap

A gap inside two minutes is sent again on the same worker, so the running handler is not cancelled. Fetch is the exception: a lost fetch reply ends that worker name, and another name in the same process picks the task up after the orphan window. A gap longer than `heartbeat_secs * missed_heartbeats` (60 seconds at the defaults) can still run the body twice. The score stays where it was while Redis accepts nothing, and another worker's heartbeat requeues the inflight set.

## Supervisor exit

On Linux a worker notices that its supervisor process has exited, including `SIGKILL`, and then drains the way `SIGUSR1` does. The watch starts after the descriptor is claimed and the Tokio runtime is up, and before the Python application is imported. It sees an exit. It does not see a supervisor that is still running but stuck.

The child is not forced out when it is not scheduled, when the Rust lifecycle is blocked, when startup has not reached that runtime yet, or when the existing drain or the Python `shutdown` hook does not return. There is no extra deadline and no extra kill. A kernel without `pidfd_open` does not start the supervisor. Other operating systems keep the previous behavior.

## Queue names

A queue name is one lowercase ASCII word of letters and digits. `default` is the example. An empty name, an uppercase letter, a hyphen, or an underscore is rejected at enqueue and again when a worker parses the message.

## Also true

Wheels are Linux x86_64 and aarch64 for CPython 3.12, 3.13, 3.14, and 3.15, each with the GIL. Free-threaded builds are not published. The worker wheel is `manylinux_2_28` (glibc 2.28 or newer) and links `libpython3.X.so.1.0` for that minor. The installed `agent-worker` command is a launcher in front of `agent-worker.bin`, because a uv-managed CPython does not put that library on the default linker path. The launcher reads `version` from `pyvenv.cfg`.

A Mac can build the producer with `maturin develop` and the worker with `cargo build -p agent-worker` against a shared CPython (`libpython3.X.dylib`). PyO3 records that library directory as an rpath, so the Linux launcher is not part of a local build. There is no macOS wheel. The cgroup ratio stays off. Per-child RSS limits use `proc_pid_rusage`. A missing `/etc/hostname` makes the worker name use `localhost`.

`MemoryClient.drain` runs one message at a time. Two handlers are never in flight together, so a test cannot show two tasks holding locks at once. A `retry` outcome is queued again immediately. The backoff delay is not waited out.

A handler time limit is delivered as `CancelledError`. `except Exception` in the handler does not see it. The last attempt is `on_dead` with reason `timeout`.

Messages are version 1. A missing version or any other version is rejected and dead-lettered with reason `invalid`. `error` on that letter is unset.

A lost Redis lease does not stop the coroutine that is already running. Cancellation is requested on the loop thread. Native code that holds the GIL runs until it releases the GIL. The ping budget is what exits that child.

A future `Config(metrics_prefix=...)` is not implemented. `dramatiq_*` names and labels stay until 2.0.
