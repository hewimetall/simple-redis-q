# Migration from Dramatiq

These pages describe simple-q 1.0.

Metric names stay. The middleware API does not.

`dramatiq_*` names and the `queue_name` / `actor_name` labels are a stable API until 2.0. Dashboards and alerts that read those series keep working. A future `Config(metrics_prefix=...)` is not implemented, so 1.0 does not rename them.

The accounting matches the fixed Prometheus middleware, not a second naming scheme:

- `queue_name` is canonical. A `.DQ` or `.XQ` suffix is removed before the label is written.
- `dramatiq_messages_inprogress` and `dramatiq_delayed_messages_inprogress` use `livesum`.
- A retry increments `dramatiq_message_retries_total` once, when the retry is enqueued with a delay.
- A delay that lives in Redis does not move `dramatiq_delayed_messages_inprogress`. That gauge is only the message this process is holding.

Dramatiq middleware does not plug in. There is no middleware chain to append, no broker middleware, and no `before_process_message` / `after_process_message` hook of that shape. The 1.0 hooks are the ones on `Client` and `App`:

| Dramatiq idea | 1.0 |
| --- | --- |
| Actor decorator that binds a broker | `@actor(...)` returns an immutable declaration, bound to neither an `App` nor a connection |
| `actor.send` | `await client.send` and `await client.send_with_options` |
| Middleware that edits a message at send | `client.on_send` |
| Middleware around the worker call | `App(..., around_dispatch=...)` |
| Retries and `age_limit` style failure | `max_retries` on `@actor`, then `on_dead(letter: DeadLetter)` |
| A synchronous `send` | none; send is async |

`on_dead` receives one `DeadLetter`. `error` is set for `retries` and `timeout` only.

Poison is `deliveries > max_deliveries`. The first attempt that lands on the limit still runs. That counter is separate from Dramatiq's retry count. `retries` is the handler-failure count. `deliveries` is how many times a worker has started the attempt, including a start after the process died.

The worker is `agent-worker worker --app module:create_app --factory` or `--app module:app`. `--app` is required. Queue names are one lowercase ASCII word of letters and digits. The example is `default`.
