# API

These pages describe simple-q 1.0. The signatures below are the supported surface. `Config.from_env()` reads `SIMPLE_Q_*` only. An invalid value raises. There is no silent default.

## `@actor`

```python
def actor(
    fn=None,
    *,
    name: str | None = None,
    queue: str = "default",
    max_retries: int = 0,
    time_limit: int = 1_800_000,
    min_backoff: int = 15_000,
    max_backoff: int = 86_400_000 * 7,
    lock_key: str | None = None,
    lock_ttl: int = 30,
    skip_on_lock: bool = False,
    max_deliveries: int = 5,
):
    """Return an immutable actor declaration.

    The declaration is bound to neither an App nor a connection.
    ``name`` defaults to the function name. ``queue`` is one lowercase
    ASCII word of letters and digits. The example queue is ``default``.
    ``max_retries`` is the number of extra attempts after the first
    failure. Zero means a failure is dead immediately.
    ``time_limit``, ``min_backoff``, and ``max_backoff`` are milliseconds.
    ``lock_key`` may contain ``{field}`` placeholders filled from kwargs
    at dispatch. The example key is ``cache:discovery``.
    ``max_deliveries`` is the poison limit. The delivery that lands on
    the limit still runs.
    """
```

`lock_key`, after substitution, is a non-empty ASCII string of letters, digits, and `:._-`. It must not start with `lock:`. A missing placeholder raises `KeyError`. Any other shape raises `ValueError`. The Redis key is `lock:` plus that string. `lock_ttl` is seconds. The default is 30.

When the lock is busy and `skip_on_lock` is true, the delivery counts as processed and does not count as an error. When `skip_on_lock` is false, a busy lock fails the attempt and follows `max_retries`. A failed unlock after a successful handler keeps the successful result.

Retry delay follows the Dramatiq 1.18 backoff. `retries` is the number already recorded, before this delay is added. The delay is `int(half + uniform(0, half))`, where `half` is half of `min(min_backoff * 2**retries, max_backoff)`.

## `Client`

```python
class Client:
    def __init__(self, url: str, config: Config | None = None) -> None:
        """Open Redis at ``url``.

        ``config`` supplies the heartbeat window, delivery limit, and
        unique-key TTL used by this process. Importing ``simple_q``
        does not open Redis. This call does.
        """

    def on_send(self, fn):
        """Register a hook that adjusts headers before enqueue.

        ``on_send`` hooks live on the Client. Each call adds a hook,
        and hooks run in registration order. The hook is synchronous
        and runs on the caller's task, before the Redis boundary, so a
        ``contextvars.ContextVar`` set by the caller is visible.
        Call shape: ``fn(actor_name, kwargs, headers) -> headers``.
        Return the headers mapping to store. ``None`` keeps the mapping
        from the previous hook. Keys and values are strings.
        """

    async def send(self, actor, /, **kwargs) -> str:
        """Enqueue ``actor`` and return the message id.

        Raises when the message was not queued, including when a
        ``unique_key`` passed another way is already held.
        """

    async def send_with_options(
        self,
        actor,
        *,
        kwargs: dict | None = None,
        delay: int | None = None,
        unique_key: str | None = None,
        headers: dict | None = None,
    ) -> str | None:
        """Enqueue ``actor`` and return the message id.

        ``delay`` is milliseconds. A missing or zero delay runs as soon
        as a worker can take the message. ``unique_key`` is claimed only
        on the first send (``retries == 0``). The example is
        ``recalc:42``. An empty string raises ``ValueError``. A second
        send of a key that is still held returns ``None`` and does not
        enqueue. A retry of the same message does not claim the key
        again. ``headers`` is ``dict[str, str]`` and is passed through
        the ``on_send`` hooks before the message is stored.
        """

    async def worker_alive(self, name: str) -> bool:
        """Return whether ``name`` still has a fresh heartbeat.

        The window is ``SIMPLE_Q_HEARTBEAT_SECS`` times
        ``SIMPLE_Q_MISSED_HEARTBEATS`` from this client's config, which
        must match the workers. The score is read from ``{queue}:workers``
        with Redis ``TIME``. An unparseable name is ``False``.
        """

    async def close(self) -> None:
        """Close the Redis connection. Later sends fail."""
```

There is no synchronous send. `send` and `send_with_options` are async.

## `App`

```python
class App:
    def __init__(
        self,
        *,
        client: Client,
        actors,
        on_start=None,
        on_stop=None,
        on_dead=None,
        around_dispatch=None,
        tracing=None,
    ):
        """Bind actor declarations to ``client``.

        Every argument is keyword-only.
        ``actors`` is a sequence of declarations from ``@actor``.
        A duplicate actor name is an error.
        ``on_start`` and ``on_stop`` are called in the child around the
        worker loop. A failure there fails that hook.
        ``on_dead`` is ``on_dead(letter: DeadLetter)`` only.
        ``around_dispatch`` is ``async def hook(message, call_next)``.
        ``current_message()`` is already bound when the hook starts.
        The hook wraps the handler and the retry or dead decision.
        """
```

The child loads this object from `--app module:app`, or from the object returned by `--app module:create_app --factory`. It calls `startup` and `shutdown` around the loop, which invoke `on_start` and `on_stop`. A dead-letter hook that raises is logged. The message stays dead.

`tracing` on the object is optional. See [Tracing and the admin board](operations.md).

Inside a handler, `current_message()` returns the message bound for this delivery, and `worker_name()` returns the worker name on that message. Outside a handler, `worker_name()` is empty.

```python
def current_message() -> CurrentMessage | None:
    """Return the message bound for the running handler, or ``None``."""

def worker_name() -> str | None:
    """Return the worker name on the current message.

    Empty outside a handler. The name is the id stored in
    ``{queue}:workers``.
    """
```

`CurrentMessage` carries `message_id`, `retries`, `max_retries`, `queue_name`, `actor_name`, `kwargs`, `headers`, `deliveries`, and `worker_name`.

## `Config`

```python
class Config:
    heartbeat_secs: int
    missed_heartbeats: int
    ping_budget_secs: int
    done_ttl_secs: int
    dead_ttl_secs: int
    unique_ttl_secs: int
    max_deliveries: int
    child_memory_soft: int | None
    child_memory_hard: int | None
    memory_soft_ratio: float
    memory_hard_ratio: float

    @classmethod
    def from_env(cls) -> "Config":
        """Read ``SIMPLE_Q_*`` once and return it.

        An invalid value raises. There is no silent default: a bad
        number, a zero where the setting must be positive, or a ratio
        outside ``(0, 1)`` raises. An omitted variable uses the default
        in the settings table. Scrape bind address, board address, and
        ``REDIS_URL`` are not fields of this object.
        ``child_memory_soft`` and ``child_memory_hard`` are bytes.
        """
```

The Redis URL is an argument of `Client`. `import simple_q` does not read `REDIS_URL`. The worker command uses the client on the `App`. `agent-worker enqueue` and `agent-worker admin` read `REDIS_URL`. `await client.worker_alive` uses this process's heartbeat window. A client that has not connected yet returns `False`.

## `QueueAdmin`

`import simple_q` does not export this type. Import the module directly.

```python
from simple_q.admin import QueueAdmin

admin = QueueAdmin(client)
snap = await admin.snapshot(["default", "quota"])
snap = await admin.snapshot(["quota"], host="worker-quota-6f8b9c7d4-x2k9p")
```

`snapshot` is async, runs on the caller's loop, and uses that `Client`'s Redis connection. It reads counters, heartbeats, and inflight tasks. It does not walk the waiting, done, or dead lists. `host` limits supervisors, their children, and orphans. Queue counters stay global for the named queues.

The return value is a frozen `Snapshot`:

- `Snapshot`: `taken_at`, `queues`, `supervisors`, `orphans`
- `QueueStats`: `name`, `active`, `scheduled`, `inflight`, `done`, `dead`, `workers`, `workers_alive`
- `SupervisorInfo`: `host`, `pid`, `started_at`, `last_seen`, `version`, `queues`, `processes`, `concurrency`, `max_tasks`, `child_soft`, `child_hard`, `cgroup_current`, `cgroup_max`, `recycled`, `drains`, `kills`, `restarts`, `children`
- `ProcessInfo`: `host`, `pid`, `ppid`, `slot`, `state`, `reason`, `exit`, `rss`, `started_at`, `ended_at`, `tasks_done`, `max_tasks`, `workers`, `last_seen`, `alive`, `tasks`
- `HeldTask`: `queue`, `task_id`, `message_id`, `actor_name`, `kwargs`, `retries`, `max_retries`, `time_limit_ms`, `locked_at`, `deliveries`

`agent-worker info --json` prints this same object. How to read `state` and `reason` is on the [tracing and admin](operations.md) page.

A future `Config(metrics_prefix=...)` is not implemented. `dramatiq_*` metric names and labels stay as they are through 1.0. See [Settings](settings.md).

## `MemoryClient`

```python
class MemoryClient:
    """In-process stand-in for ``Client``. Sending does not open Redis.

    ``send`` and ``send_with_options`` queue in this process.
    ``worker_alive`` is ``True``. A ``unique_key`` is held while its
    message is queued or being drained. A second send of that key
    returns ``None`` until ``drain`` finishes it.
    """

    async def drain(self, app: App) -> None:
        """Run every queued message through ``app``.

        Messages sent by a handler are included. The unique key is
        released when dispatch returns an outcome other than ``retry``,
        and when dispatch raises. A ``retry`` outcome leaves the key
        held and does not put the message back on the in-process queue.
        The call ends when that queue is empty.
        """
```

## `DeadLetter`

```python
class DeadLetter:
    """One dead message. ``on_dead`` receives this object and nothing else."""

    actor_name: str
    kwargs: dict
    reason: str
    message_id: str
    retries: int
    deliveries: int
    error: str | None
```

`error` is `"{Type}: {message}"` when the reason is `retries` or `timeout`. `error` is unset for `poison`, `invalid`, and `unknown_actor`.

| `reason` | When | `error` |
| --- | --- | --- |
| `retries` | The handler raised and `retries` is already at `max_retries` | set |
| `timeout` | The handler exceeded `time_limit` on the last attempt | set |
| `poison` | `deliveries > max_deliveries` before the handler | unset |
| `invalid` | The stored message failed validation | unset |
| `unknown_actor` | `actor_name` is not on this `App` | unset |

`kwargs` on the letter is a copy of the message kwargs.
