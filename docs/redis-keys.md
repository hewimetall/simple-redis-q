# Redis keys

These pages describe simple-q 1.0.

Queue names in keys are one lowercase ASCII word. The examples use `default`. The unique-key example is `recalc:42`. The lock-key example is `cache:discovery`.

| Key | Role |
| --- | --- |
| `{queue}:workers` | Sorted set of worker heartbeats. The score is Redis `TIME`. `await client.worker_alive(name)` reads `ZSCORE` here. |
| `{queue}:workers:metadata:{name}` | Hash Apalis writes for a worker, with no TTL of its own. The child adds `ppid`, `slot`, `started_at`, `max_tasks`, and `tasks_done`. An orderly child exit deletes the whole hash. A sweep deletes a hash whose name is no longer in `{queue}:workers`. |
| `simple_q:supervisors` | Sorted set of supervisor heartbeats. The member is `{host}.{pid}`. The score is Redis `TIME`. A supervisor removes members older than one heartbeat window. |
| `simple_q:supervisor:{host}.{pid}` | Hash for one supervisor. `EXPIRE` is two heartbeat windows. Static fields are `host`, `pid`, `started_at`, `version`, `queues`, `processes`, `concurrency`, `max_tasks`, `child_soft`, and `child_hard`. Each heartbeat refreshes `last_seen`, `cgroup_current`, `cgroup_max`, `recycled`, `drains`, `kills`, and `restarts`. `child:{pid}` is JSON `{slot, state, reason, rss, started_at, ended_at, exit, tasks_done, max_tasks}`. The child also writes `tasks:{pid}` with the latest `tasks_done`. The next publish copies that count into the child JSON, which is what remains after an orderly exit deletes the metadata hash. The supervisor deletes a finished child row after two heartbeat windows. |
| `{queue}:idempotency:{key}` | `SETNX` claim for `unique_key`. `default:idempotency:recalc:42` is the example. TTL is `SIMPLE_Q_UNIQUE_TTL_SECS` (86400). Claimed only when `retries` is 0 and the key is non-empty. A retry does not claim it again. |
| `{queue}:inflight:{worker_name}` | Messages a worker has taken and not acked. A live worker's heartbeat can requeue another worker's abandoned inflight. That requeue does not increment the delivery counter. |
| `{queue}:meta:{task_id}` | Hash Apalis creates for one task. This runtime `HSET`s `started_at` to Redis `TIME` seconds when a delivery starts. Apalis overwrites `locked_at` on each lease renew and does not change `started_at`. The next delivery overwrites `started_at`. A dead delivery `HSET`s `reason`, `dead_at`, `deliveries`, and `error` when there is one. Stored `error` is at most 4096 bytes. |
| `{queue}:dead` | Dead task ids. Apalis owns the key. A new key is a sorted set scored by completion time. A key that is already a set stays a set. The sweeper deletes a record older than `SIMPLE_Q_DEAD_TTL_SECS` (7 days by default), together with its payload, its per-task meta hash, and the index member below. |
| `simple_q:dead:{queue}` | Sorted set this runtime owns. The score is Redis `TIME` seconds. The member is the task id. `QueueAdmin.dead` pages it newest-first. The sweeper `ZREM`s the member when it deletes that dead task. A dead id written before this index existed is not a member. It stays in `{queue}:dead` until the sweeper deletes it. |
| `delivery:{message_id}:{retries}` | Delivery counter, outside the queue keyspace. Example: `delivery:3f1c0b2a-...:0`. `INCR` and `EXPIRE` run in one script. The crash TTL on this key is 7 days. |

`{queue}:done` is the success key, with the same two shapes as `{queue}:dead`. The sweeper deletes a record older than `SIMPLE_Q_DONE_TTL_SECS` (24 hours by default) in the same cursor walk. The walk uses `ZSCAN` or `SSCAN`, and `HSCAN` for worker metadata. It removes the payload, the per-task meta hash, and the member together. A dead id is removed from `simple_q:dead:{queue}` in that same delete.

The business lock is `lock:{lock_key}`, for example `lock:cache:discovery`. It is not an Apalis queue key. The message lease and the business lock are different keys. Losing the lease does not unlock the business lock. The TTL is extended until the owner releases the key or the ping budget elapses. A single missed ping does not stop that extend. A failed extend is retried. When the budget elapses, renewal stops. `lock_ttl` is seconds and must be greater than zero.

`unique_key` is deleted on a terminal outcome: ack, dead, or poison. A retry leaves the idempotency key in place. A crash before that outcome leaves it until the TTL. Poison and a dead outcome leave `delivery:{message_id}:{retries}` in place so the next start of that attempt still sees a count above `max_deliveries`. Ack, and a retry that has been enqueued, delete the delivery key immediately.

The first send of `recalc:42` on `default` writes `default:idempotency:recalc:42`. A second send while that key is held returns no message id.
