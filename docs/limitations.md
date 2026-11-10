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

Wheels are Linux x86_64 and aarch64 for CPython 3.12.

Messages are version 1. A missing version or any other version is rejected and dead-lettered with reason `invalid`. `error` on that letter is unset.

A lost Redis lease does not stop the coroutine that is already running. Cancellation is requested on the loop thread. Native code that holds the GIL runs until it releases the GIL. The ping budget is what exits that child.

A future `Config(metrics_prefix=...)` is not implemented. `dramatiq_*` names and labels stay until 2.0.
