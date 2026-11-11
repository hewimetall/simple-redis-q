# Changelog

## 1.0.0rc3

The worker wheel is `manylinux_2_28`. `agent-worker` in a virtualenv is a launcher that points `LD_LIBRARY_PATH` at the base interpreter's `lib` before `agent-worker.bin`.

`SIMPLE_Q_APP_FACTORY` turns `--factory` on only for `1`, `true`, `yes`, and `on`.

`from simple_q import QueueAdmin` loads the extension. `import simple_q` still does not.

`MemoryClient.drain` calls `startup` and `shutdown`, and puts a `retry` outcome back on the queue. `worker_alive` is false until `set_worker_alive`. A time limit still arrives in the handler as `CancelledError`.

## 1.0.0rc2

The supervisor writes its own Redis record. `QueueAdmin.snapshot` and `agent-worker info` read supervisors, processes, and the tasks those processes hold. They read counters, heartbeats, and inflight tasks.

## 1.0.0rc1

`Client`, `App`, `Config`, and `@actor` replace `configure`, `Registry`, and the memory globals. `on_dead` takes a `DeadLetter`. `worker_alive` is async on the client.
