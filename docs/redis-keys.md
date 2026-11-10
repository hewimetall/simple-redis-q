# Redis keys

These pages describe simple-q 1.0.

Queue names in keys are one lowercase ASCII word. The examples use `default`. The unique-key example is `recalc:42`. The lock-key example is `cache:discovery`.

| Key | Role |
| --- | --- |
| `{queue}:workers` | Sorted set of worker heartbeats. The score is Redis `TIME`. `await client.worker_alive(name)` reads `ZSCORE` here. |
| `{queue}:workers:metadata:{name}` | Hash Apalis writes for a worker, with no TTL of its own. An orderly child exit deletes it. A sweep deletes a hash whose name is no longer in `{queue}:workers`. |
| `{queue}:idempotency:{key}` | `SETNX` claim for `unique_key`. `default:idempotency:recalc:42` is the example. TTL is `SIMPLE_Q_UNIQUE_TTL_SECS` (86400). Claimed only when `retries` is 0 and the key is non-empty. A retry does not claim it again. |
| `{queue}:inflight:{worker_name}` | Messages a worker has taken and not acked. A live worker's heartbeat can requeue another worker's abandoned inflight. That requeue does not increment the delivery counter. |
| `{queue}:dead` | Set of dead message ids. The sweeper deletes a record older than `SIMPLE_Q_DEAD_TTL_SECS` (7 days by default), together with its payload and per-task meta hash. |
| `delivery:{message_id}:{retries}` | Delivery counter, outside the queue keyspace. Example: `delivery:3f1c0b2a-...:0`. `INCR` and `EXPIRE` run in one script. The crash TTL on this key is 7 days. |

`{queue}:done` is the success set. The sweeper deletes a record older than `SIMPLE_Q_DONE_TTL_SECS` (24 hours by default) in the same cursor walk. The walk uses `SSCAN` and `HSCAN`. It removes the payload, the per-task meta hash, and the set member together.

The business lock is `lock:{lock_key}`, for example `lock:cache:discovery`. It is not an Apalis queue key. The message lease and the business lock are different keys. Losing the lease does not unlock the business lock, and the lock TTL is renewed while the Python loop is healthy. When the ping budget elapses, renewal stops.

`unique_key` is deleted on a terminal outcome: ack, dead, or poison. A retry leaves the idempotency key in place. A crash before that outcome leaves it until the TTL. Poison and a dead outcome leave `delivery:{message_id}:{retries}` in place so the next start of that attempt still sees a count above `max_deliveries`. Ack, and a retry that has been enqueued, delete the delivery key immediately.

The first send of `recalc:42` on `default` writes `default:idempotency:recalc:42`. A second send while that key is held returns no message id.
