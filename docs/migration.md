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
| Middleware around the worker call | `App(client=..., around_dispatch=...)` |
| `before_process_message` / `after_process_message` | one `around_dispatch` hook, before and after `await call_next()` |
| `after_skip_message` | the same hook reads `outcome == "skipped"` |
| `after_worker_boot` / `before_worker_shutdown` | `on_start` / `on_stop` in the child |
| Retries and `age_limit` style failure | `max_retries` on `@actor`, then `on_dead(letter: DeadLetter)`. An `around_dispatch` hook can replace `delay_ms` on a `retry` outcome |
| `message.options` for one send | a keyword of `await client.send`, or `headers` when the handler does not take that key |
| An actor option declared by middleware | a value in the actor's module. `@actor` has no extra keywords |
| A synchronous `send` | none; send is async |

Hooks are functions. A class with `install` is the middleware shape:

```python
class AlterMiddleware:
    def install(self, client, app):
        client.on_send(self.on_send)
        app.around_dispatch(self.around)
        app.on_start(self.on_start)
        app.on_stop(self.on_stop)
        app.on_dead(self.on_dead)

    def on_send(self, actor_name, kwargs, headers):
        updated = dict(headers)
        updated["alter"] = "1" if kwargs.get("alter") is True else "0"
        return updated

    async def around(self, message, call_next):
        return await call_next()
```

`on_send` runs in registration order. The first `around_dispatch` hook registered is the outer hook. `await client.send(recalc, project_id="42", alter=True)` stores `alter` in `kwargs` as JSON. The handler must accept `alter`. `headers` is `dict[str, str]`, so a boolean option is stored as `"1"` or `"0"`. `@actor(alter=True)` raises `TypeError`. A flag that is the same for every send of one actor lives in the module the worker imports, and `on_send` copies it onto the message when the delivery should see it.

`on_dead` runs inside `call_next`, before an outer hook sees the outcome dict. An exception from `dispatch` kills the attempt and releases the idempotency key. Python does not enqueue a retry for that exception.

`python/examples/alter_option.py` is a working middleware. It sends `alter` as a handler argument, copies it into a header, and passes `note` as a header the handler does not take. From the repository root:

```bash
PYTHONPATH=python python python/examples/alter_option.py
```

`on_dead` receives one `DeadLetter`. `error` is set for `retries` and `timeout` only.

Poison is `deliveries > max_deliveries`. The first attempt that lands on the limit still runs. That counter is separate from Dramatiq's retry count. `retries` is the handler-failure count. `deliveries` is how many times a worker has started the attempt, including a start after the process died.

The worker is `agent-worker worker --app module:create_app --factory` or `--app module:app`. `--app` is required unless `SIMPLE_Q_APP` is set. Queue names are one lowercase ASCII word of letters and digits. The example is `default`.
