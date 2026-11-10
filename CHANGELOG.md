# Changelog

## 1.0.0rc2

The supervisor writes its own Redis record. `QueueAdmin.snapshot` and `agent-worker info` read supervisors, processes, and the tasks those processes hold. They read counters, heartbeats, and inflight tasks.

## 1.0.0rc1

`Client`, `App`, `Config`, and `@actor` replace `configure`, `Registry`, and the memory globals. `on_dead` takes a `DeadLetter`. `worker_alive` is async on the client.
