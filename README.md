# simple-q

Rust supervisor and queue runtime for Python async tasks. The worker binary is `agent-worker`. Application code imports `simple_q`. The Rust extension inside that package is `simple_q._runtime`.

## Install

Supported runtime: CPython 3.12, Linux x86_64 and aarch64, glibc 2.28 or newer.

```bash
uv add simple-q-runtime simple-q-worker
```

`simple-q-runtime` is the importable package (`import simple_q`). `simple-q-worker` installs the `agent-worker` binary into the virtualenv and depends on the same version of `simple-q-runtime`.

## Example

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

## Documentation

These pages describe 1.0: [docs/index.md](docs/index.md).
