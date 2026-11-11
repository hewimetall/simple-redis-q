# Quick start

These pages describe simple-q 1.0.

simple-q runs Python async tasks on Redis. `simple-q-runtime` is the importable package (`import simple_q`). `simple-q-worker` installs the `agent-worker` supervisor. Importing `simple_q` does not open Redis. `Client` does.

Supported runtime: CPython 3.12, 3.13, 3.14, and 3.15 with the GIL, Linux x86_64 and aarch64. Free-threaded builds are not supported. A producer and a worker may use different minor versions. The message is JSON. The actor module is imported by the worker. A Mac builds the producer with `maturin develop` and a local worker with `cargo build -p agent-worker` against a shared CPython. Published wheels stay Linux.

```bash
uv add simple-q-runtime simple-q-worker
```

Declare an actor, build an `App`, and send on the `default` queue:

```python
from simple_q import App, Client, Config, actor


@actor(queue="default", max_retries=2, lock_key="cache:discovery")
async def recalc(project_id: int) -> None:
    ...


def create_app() -> App:
    config = Config.from_env()
    return App(client=Client("redis://redis:6379/0", config=config), actors=[recalc])


async def enqueue() -> None:
    config = Config.from_env()
    client = Client("redis://redis:6379/0", config=config)
    await client.send_with_options(
        recalc,
        kwargs={"project_id": 42},
        unique_key="recalc:42",
    )
    await client.close()
```

`@actor` returns an immutable declaration. It is bound to neither an `App` nor a connection. `Config.from_env()` reads the `SIMPLE_Q_*` settings. An invalid value raises. There is no silent default. The Redis URL is an argument of `Client`. `agent-worker enqueue` and `agent-worker admin` are the commands that read `REDIS_URL`.

Start the supervisor with the factory that returns the `App`. `--app` is required unless `SIMPLE_Q_APP` is set.

```bash
agent-worker worker --app module:create_app --factory --queues default
```

An application object works the same way, without `--factory`:

```bash
agent-worker worker --app module:app --queues default
```

The containers inside the runtime are Client, App, Config, the worker supervisor, and Redis. The context diagram is on the [architecture](architecture.md) page.

Next: [concepts](concepts.md), the [API](api.md), [settings](settings.md), and [deploy](deploy.md).
