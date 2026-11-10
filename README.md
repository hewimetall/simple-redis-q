# agent-worker

Rust supervisor and embedded CPython 3.12 runtime for registered Python async tasks.

The binary is `agent-worker`. The importable Python package is `simple_q`. The Rust module, embedded in the worker and also built as a wheel, is `simple_q_runtime`.

Importing `simple_q` does not open Redis. Call `simple_q.configure(redis_url=...)` after the process is the one that will send, including after a gunicorn fork. `send` before that call fails.

Product queues are `default` and `quota`. The decision for replacing Dramatiq is recorded in [docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md](docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md).

Metrics keep the Dramatiq names and the accounting of the fixed Prometheus middleware: canonical `queue_name`, both gauges in `livesum`, and a retry counted once when it is enqueued with a delay. Redis-scheduled retries are not in-process memory, so they do not move `dramatiq_delayed_messages_inprogress`. The scrape listens on `DRAMATIQ_PROM_HOST`:`DRAMATIQ_PROM_PORT` (default `0.0.0.0:9191`) and any path. `DRAMATIQ_PROM_DB` is the multiprocess directory. The supervisor wipes `*.db` once; each child writes files under `SIMPLE_Q_SLOT`.

`agent-worker admin` listens on `127.0.0.1:8080` and requires `BOARD_TOKEN` (at least 16 letters or digits), including on loopback. `BOARD_INSECURE=1` omits the token only for a loopback address. Open the board once as `/?token=...`; the token is then kept in an HttpOnly cookie. The compose file publishes that port on the host loopback and still requires `BOARD_TOKEN`, because the process inside the container listens on the container network.

Redis in compose has no password and is published on `127.0.0.1:6379`. Anyone who can reach it can put work on a queue. Set `requirepass` before sharing that port.

## Tracing

The child does not open an OTLP connection. Pass `Tracing` to `Registry` and Rust spans are handed to that Python exporter on a dedicated thread. `tracing=None` is the default: no OpenTelemetry layer is installed, and Python is not called to export spans.

```python
from opentelemetry.sdk.resources import Resource

from simple_q import Registry, Tracing

app = Registry(
    tracing=Tracing(
        exporter=exporter,  # export(spans) and shutdown()
        resource=Resource.create({"service.name": "agent-worker"}),
        targets={"agent_worker": "info", "simple_q_runtime": "info", "apalis": "debug"},
    )
)
```

`RUST_LOG` filters the text log. `targets` filters the OpenTelemetry layer, so `RUST_LOG=warn` still exports the process span.

## Install

```bash
uv add simple-q-runtime simple-q-worker
```

`simple-q-runtime` is the importable package (`import simple_q`). `simple-q-worker` installs the `agent-worker` binary into the virtualenv and depends on the same version of `simple-q-runtime`. The binary is placed at `.venv/bin/agent-worker`. CPython locates that environment from `pyvenv.cfg` one directory above the executable, so the installed package imports without `PYTHONPATH`.

Wheels are Linux only, for x86_64 and aarch64. The extension wheel is a manylinux build. The binary wheel is built in `python:3.12-slim-bookworm` and tagged `manylinux_2_36`, because the binary links `libpython3.12.so.1.0`. Publish steps are in [docs/dev/PUBLISH.md](docs/dev/PUBLISH.md).
