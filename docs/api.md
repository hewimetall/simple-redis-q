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

When the lock is busy and `skip_on_lock` is true, the delivery counts as processed and does not count as an error. When `skip_on_lock` is false, a busy lock fails the attempt and follows `max_retries`. A failed unlock after a successful handler keeps the successful result. `lock_ttl` must be greater than zero. Cancelling `dispatch` while the unlock is waiting still releases the lock.

Retry delay follows the Dramatiq 1.18 backoff. `retries` is the number already recorded, before this delay is added. The delay is `int(half + uniform(0, half))`, where `half` is half of `min(min_backoff * 2**retries, max_backoff)`.

`@actor` accepts only the parameters in its signature. `@actor(alter=True)` raises `TypeError`. A value that changes from send to send is a keyword of `send` or an entry in `headers`. A value that is the same for every send of one actor lives in the module that declares the actor. The worker imports that module. It is written onto the message when `on_send` copies it into `headers`, or when the caller includes it in `kwargs`.

`python/examples/alter_option.py` is that shape. `alter` is an argument of the handler. The middleware copies it into the string header `alter`. `note` is a header the handler does not take as an argument. `lane` comes from a dict in the example module. From the repository root:

```bash
PYTHONPATH=python python python/examples/alter_option.py
```

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

        ``kwargs`` are the handler arguments. They are stored as JSON:
        strings, numbers, booleans, lists, and objects. The handler is
        called as ``function(**kwargs)``, so it must accept every key.
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

Several middleware classes share one `App`. Each `install` appends that class's hooks. The list order is the stack order. For classes installed as `a`, then `b`, then `c`, then `d`:

- `on_send`, `on_start`, `on_stop`, and `on_dead` run `a`, `b`, `c`, `d`.
- Each `on_send` receives the headers returned by the previous hook. `None` keeps that mapping. The same header key keeps the last write.
- `around_dispatch` enters `a`, `b`, `c`, `d`, then the handler, then leaves `d`, `c`, `b`, `a`. The first registered hook is the outer hook. `current_message()` is already bound when that outer hook starts.
- An outer hook that returns without calling `call_next` leaves the inner hooks and the handler uncalled.
- The dict an inner hook returns is the dict the outer hook receives. A `delay_ms` written on a `retry` outcome is visible to every outer hook, and it is the dict `dispatch` returns.
- `on_dead` runs inside the handler path, after the handler and before any `around_dispatch` hook leaves. An exception from one `on_start`, `on_stop`, or `on_dead` hook is logged. The later hooks in that list still run.

`python/tests/test_middleware_stack.py` installs four classes and records this order.

`dispatch` returns a dict. The worker reads `outcome`:

| `outcome` | What the worker does |
| --- | --- |
| `retry` | Enqueues the same message after `delay_ms` milliseconds and adds one to `retries`. A missing `delay_ms` waits zero. The idempotency key stays. |
| `dead` | Releases the idempotency key and records the attempt as killed. `reason` is the abort text. `error` is the exception text for `retries` and `timeout`, and null for `poison` and `unknown_actor`. The delivery key stays. Rust stores `reason`, `error`, `dead_at`, and `deliveries`. Stored `error` is at most 4096 bytes. |
| any other value, including `ok` and `skipped` | Acknowledges the delivery and releases the idempotency key. |

An `around_dispatch` hook can return one of those dicts without calling `call_next`. An exception from `dispatch` releases the idempotency key and kills the attempt. Python does not enqueue a retry for that exception. `on_dead` runs inside the handler path, before the outer `around_dispatch` hook sees the dict. Replacing a `dead` outcome with `retry` after that leaves the dead-letter hook already called.

A middleware class registers the hooks in `install(client, app)`. The running example is `python/examples/alter_option.py`. The Dramatiq names for the same hooks are on the [migration](migration.md) page.

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

`import simple_q` does not load this type. Importing the name does, and that loads the extension.

```python
from simple_q import QueueAdmin

admin = QueueAdmin(client)
snap = await admin.snapshot(["default", "quota"])
snap = await admin.snapshot(["quota"], host="worker-quota-6f8b9c7d4-x2k9p")
```

`snapshot` is async, runs on the caller's loop, and uses that `Client`'s Redis connection. It reads counters, heartbeats, and inflight tasks. It does not walk the waiting, done, or dead lists. `host` limits supervisors, their children, and orphans. Queue counters stay limited to the queues in the argument. A process still lists held tasks for every queue stored on its supervisor record, including a queue the caller did not pass. Those extra queues are not added to `Snapshot.queues`.

The return value is a frozen `Snapshot`:

- `Snapshot`: `taken_at`, `queues`, `supervisors`, `orphans`
- `QueueStats`: `name`, `active`, `scheduled`, `inflight`, `done`, `dead`, `workers`, `workers_alive`
- `SupervisorInfo`: `host`, `pid`, `started_at`, `last_seen`, `version`, `queues`, `processes`, `concurrency`, `max_tasks`, `child_soft`, `child_hard`, `cgroup_current`, `cgroup_max`, `recycled`, `drains`, `kills`, `restarts`, `children`
- `ProcessInfo`: `host`, `pid`, `ppid`, `slot`, `state`, `reason`, `exit`, `rss`, `started_at`, `ended_at`, `tasks_done`, `max_tasks`, `workers`, `last_seen`, `alive`, `tasks`
- `HeldTask`: `queue`, `task_id`, `message_id`, `actor_name`, `kwargs`, `retries`, `max_retries`, `time_limit_ms`, `locked_at`, `started_at`, `deliveries`

`agent-worker info --json` prints this same object. How to read `state` and `reason` is on the [tracing and admin](operations.md) page.

### Field meanings

- `rss` is resident bytes from `/proc/{pid}/statm` (resident pages times the page size). Shared libraries are mapped into every process that loads them, so those pages count in every process.
- `cgroup_current` is `memory.current` minus `inactive_file`. `cgroup_max` is `memory.max` when that file is a byte count. The value published on the supervisor record is refreshed on the supervisor heartbeat. The one-second read is only the memory-limit check. It does not publish a new `cgroup_current`.
- `tasks_done` includes poison. The child writes it on its heartbeat, to `tasks_done` on the worker metadata hash and to `tasks:{pid}` on the supervisor hash.
- While the child row still says running and `tasks_done` has reached `max_tasks`, the snapshot shows `draining` / `max_tasks`. After exit 0 the row is `exited` / `max_tasks`.
- `recycled` counts every exit 0, including an exit 0 after a drain.
- `drains` counts `SIGUSR1` for `memory_soft` and `memory_ratio` only. Shutdown does not increment it.
- `kills` counts only a `SIGKILL` this supervisor sent: the per-child hard limit and the hard cgroup ratio.
- `restarts` counts that `SIGKILL` immediately. It also counts a later killed `wait` that was not already counted: the ping budget (exit 2), an external `SIGKILL`, and a crash.
- `alive` is heartbeat freshness. It does not follow `ended_at`.
- `locked_at` is the absolute lease time Apalis writes on `{queue}:meta:{task_id}` and overwrites on every heartbeat renew. `started_at` is Redis `TIME` seconds when this delivery was taken. Renew does not change it. The next delivery overwrites it.

### `dead`

```python
page = await admin.dead("default")
page = await admin.dead("default", limit=50, cursor=page.cursor)
```

`dead` pages the sorted set `simple_q:dead:{queue}` newest-first. The score is Redis `TIME` seconds and the member is the task id. Apalis keeps `{queue}:dead` separately, as a sorted set scored by completion time when it creates the key, or as a set when the key already is one. This read does not scan that key. It loads one page of ids, then those fields of `{queue}:data` and `{queue}:meta:{task_id}`.

`limit` defaults to 50. `0` raises `ValueError`. A larger limit is capped at 200. `cursor` is opaque and exclusive: the task it names is not repeated. Pass the returned `cursor` back to read the next page. `None` means this page is the last.

The cursor text is base64url, without padding, of `d\n{dead_at}\n{task_id}`. `dead_at` is decimal seconds. Equal scores are ordered by task id in reverse lexicographic order, which is the order Redis `ZREVRANGE` uses.

The return value is a frozen `DeadPage`:

- `DeadPage`: `messages`, `cursor`
- `DeadMessage`: `task_id`, `message_id`, `actor_name`, `kwargs`, `reason`, `error`, `retries`, `deliveries`, `dead_at`

`reason` is `poison`, `invalid`, the Python `dead` reason (`retries`, `timeout`, `unknown_actor`, or another reason the outcome dict carried), or `dispatch` when `dispatch` raises. `error` is the exception text for `retries`, `timeout`, and `dispatch`, stored at most 4096 bytes. It is null for `poison`, `invalid`, and `unknown_actor`. `on_dead` is unchanged: it still receives one `DeadLetter`.

`dead` lists ids written to `simple_q:dead:{queue}` from 1.0.0rc5 on. A task that died before that index is still in `{queue}:dead` and still counts in the queue snapshot. `dead` does not return it. The sweeper deletes it with the rest of that key, within the dead TTL (seven days by default). The sweeper also removes an index member when it deletes that dead task.

### `peek`

```python
waiting = await admin.peek("default", "active")
later = await admin.peek("default", "scheduled", limit=50, cursor=None)
```

`peek` reads one page of `{queue}:active` with `LRANGE`, or one page of `{queue}:scheduled` with `ZRANGE` and `WITHSCORES`. It then loads only those ids from `{queue}:data`. It does not call apalis `list_tasks`. `state` is `active` or `scheduled`. Any other value raises `ValueError`. `limit` follows the same rules as `dead`. A cursor that does not decode raises `ValueError`.

Active pages start at the head of the list. The cursor is base64url, without padding, of `a\n{offset}`. `offset` is the zero-based index of the next page. A worker can take a task between pages, so a later page can skip or repeat an id. A missing id does not fail the page. A cursor that names a task id is not an offset and raises `ValueError`.

Scheduled pages are soonest-first. The cursor is base64url of `s\n{run_at}\n{task_id}` and is exclusive. Equal scores are lexicographic by task id. `run_at` is the score, in Redis seconds. Active rows leave `run_at` unset.

The return value is a frozen `TaskPage`:

- `TaskPage`: `tasks`, `cursor`
- `QueuedTask`: `task_id`, `message_id`, `actor_name`, `kwargs`, `retries`, `max_retries`, `time_limit_ms`, `run_at`

A future `Config(metrics_prefix=...)` is not implemented. `dramatiq_*` metric names and labels stay as they are through 1.0. See [Settings](settings.md).

## `MemoryClient`

```python
class MemoryClient:
    """In-process stand-in for ``Client``. Sending does not open Redis.

    ``send`` and ``send_with_options`` queue in this process.
    ``worker_alive`` is ``False`` until ``set_worker_alive(name)``.
    A ``unique_key`` is held while its message is queued, retrying, or
    being drained. A second send of that key returns ``None`` until a
    terminal outcome releases it.
    """

    def set_worker_alive(self, name: str, alive: bool = True) -> None:
        """Mark ``name`` alive, or dead when ``alive`` is ``False``."""

    async def drain(self, app: App) -> None:
        """Run every queued message through ``app``, one at a time.

        Calls ``startup`` before the first message and ``shutdown`` when
        the queue is empty or dispatch raises. Messages sent by a handler
        are included. Two handlers are never in flight together.
        A ``retry`` outcome increments ``retries`` and goes back on the
        queue immediately. The backoff is not waited out. The unique key
        stays held across that retry and is released for any other
        outcome, and when dispatch raises.
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

A time limit cancels the running handler. The handler sees `CancelledError`. That is a `BaseException`, so `except Exception` inside the handler does not run. Dispatch sees `TimeoutError`. While `retries` is below `max_retries` the outcome is `retry`. On the last attempt the outcome is `dead` with reason `timeout`, and `on_dead` runs. There is no separate failure hook.

`kwargs` on the letter is a copy of the message kwargs.
