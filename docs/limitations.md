# Limitations

These pages describe simple-q 1.0.

## No synchronous send

`Client.send` and `Client.send_with_options` are async. A caller needs a running event loop. There is no synchronous send.

## `catch_panic` is off

The worker does not enable Apalis `catch_panic`. A panic takes down the child, and the supervisor restarts it.

`catch_panic` is off because Apalis retries `Failed` on its own. A handler failure that should stay dead is returned as an abort, which the worker records as killed, and Python enqueues the retry itself when `max_retries` allows one. A panic converted into `Failed` would be scheduled again by the backend, past the actor's `max_retries`.

## Queue names

A queue name is one lowercase ASCII word of letters and digits. `default` is the example. An empty name, an uppercase letter, a hyphen, or an underscore is rejected at enqueue and again when a worker parses the message.

## Also true in 1.0

Wheels are Linux x86_64 and aarch64 for CPython 3.12, 3.13, 3.14, and 3.15, each with the GIL. Free-threaded builds are not published. The worker wheel is `manylinux_2_28` (glibc 2.28 or newer) and links `libpython3.X.so.1.0` for that minor. The installed `agent-worker` command is a launcher in front of `agent-worker.bin`, because a uv-managed CPython does not put that library on the default linker path. The launcher reads `version` from `pyvenv.cfg`.

`MemoryClient.drain` runs one message at a time. Two handlers are never in flight together, so a test cannot show two tasks holding locks at once. A `retry` outcome is queued again immediately. The backoff delay is not waited out.

A handler time limit is delivered as `CancelledError`. `except Exception` in the handler does not see it. The last attempt is `on_dead` with reason `timeout`.

Messages are version 1. A missing version or any other version is rejected and dead-lettered with reason `invalid`. `error` on that letter is unset.

A lost Redis lease does not stop the coroutine that is already running. Cancellation is requested on the loop thread. Native code that holds the GIL runs until it releases the GIL. The ping budget is what exits that child.

A future `Config(metrics_prefix=...)` is not implemented. `dramatiq_*` names and labels stay until 2.0.
