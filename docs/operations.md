# Tracing and the admin board

These pages describe simple-q 1.0.

## Tracing

The child does not open an OTLP connection. It reads `tracing` on the loaded application. `None`, or a missing attribute, installs no OpenTelemetry layer and does not call into Python to export spans.

```python
from opentelemetry.sdk.resources import Resource

from simple_q import App, Client, Config, Tracing, actor


@actor(queue="default")
async def recalc(project_id: int) -> None:
    ...


def create_app() -> App:
    config = Config.from_env()
    app = App(client=Client("redis://redis:6379/0", config=config), actors=[recalc])
    app.tracing = Tracing(
        exporter=exporter,  # export(spans) and shutdown()
        resource=Resource.create({"service.name": "agent-worker"}),
    )
    return app
```

```python
class Tracing:
    def __init__(
        self,
        *,
        exporter,
        resource,
        targets: dict[str, str] | None = None,
        max_queue_size: int = 2048,
        max_export_batch_size: int = 512,
        scheduled_delay_ms: int = 5000,
        export_timeout_ms: int = 10000,
    ):
        """Hand finished Rust spans to a Python exporter.

        ``exporter`` provides ``export(spans)`` and ``shutdown()``.
        ``resource`` is attached once, at child start, and copied onto
        each span. ``targets`` filters the OpenTelemetry layer.
        ``None`` uses ``agent_worker=info``, ``simple_q_runtime=info``,
        and ``apalis=debug``. An empty dict is rejected and the child
        exits 3. Every queue, batch, delay, and timeout value must be
        a positive integer.
        """
```

`RUST_LOG` filters the text log. It does not filter the OpenTelemetry layer, so `RUST_LOG=warn` still exports the process span.

Export runs on one OS thread. Tokio tasks do not call into Python to export. The queue is bounded by `max_queue_size`. When the interpreter holds the GIL and the queue is full, the span is dropped. The process does not grow a buffer to hold it. Exit 2 does not flush. Spans buffered for a stuck task are dropped.

Each delivery opens a process span named `process {actor_name}`. A `traceparent` header, with `tracestate` when present, is the parent. An invalid `traceparent` is not a parent. A `tracelink` header that is itself a W3C traceparent becomes a span link. The copy of the message handed to Python carries the process span's own `traceparent`. The stored message keeps the sender's headers, including across a retry.

`on_send` on the `Client` is the place to put `traceparent` on the way in. It runs on the caller's task, before Redis.

## Admin board

```bash
agent-worker admin
```

The board reads Redis and does not start a worker. `agent-worker admin` reads `REDIS_URL` and does not load the application. `QUEUE` selects the queue and defaults to `default`.

The listen address is `BOARD_ADDR`, default `127.0.0.1:8080`. `BOARD_TOKEN` is required on every address, including loopback. The token is at least 16 characters from `A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, and `-`. `BOARD_INSECURE=1` omits the token only for a loopback address.

Open the board once as `/?token=...`. A matching token on `GET`, outside `/api/`, redirects to the same path without the query and stores the token in an HttpOnly `board_token` cookie with `SameSite=Strict`. `Authorization: Bearer` is accepted on later requests. A missing token is 401 with `WWW-Authenticate: Bearer`.

Publishing the board port on a host interface still requires `BOARD_TOKEN`. The process inside a container listens on the container network, which is not the host's loopback, even when Docker publishes `127.0.0.1:8080:8080`.

The board is a view of the Redis queue. It does not replace the Prometheus scrape on port 9191.

## Reading `state` and `reason`

`agent-worker info` and `QueueAdmin.snapshot` show each child the supervisor still remembers. `state` is `running`, `draining`, `exited`, or `killed`. `reason` is why the supervisor last touched that process.

| `state` | `reason` | What happened |
| --- | --- | --- |
| `running` | empty | The child is up. The supervisor has not signaled it. |
| `draining` | `memory_soft` | `SIGUSR1` because that process crossed its soft RSS limit. |
| `draining` | `memory_ratio` | `SIGUSR1` because the container working set crossed the soft ratio, or the hard ratio when no child owned the usage. A child that does own it is `killed` / `memory_ratio` instead. |
| `draining` | `shutdown` | `SIGUSR1` because the supervisor got `SIGTERM` or `SIGINT`. |
| `draining` | `max_tasks` | The child row still says running, and `tasks_done` has reached `max_tasks`. The snapshot shows the drain before the process exits. |
| `exited` | `max_tasks` | Exit 0 and the supervisor had not sent a signal. The child finished its task budget. |
| `exited` | the drain reason | Exit 0 after the supervisor's `SIGUSR1`. A soft drain becomes `exited` / `memory_soft`. |
| `killed` | `memory_hard` | This supervisor sent `SIGKILL` because the process crossed its hard RSS limit. |
| `killed` | `memory_ratio` | This supervisor sent `SIGKILL` because the container crossed the hard ratio and that child owned the usage. |
| `killed` | `ping_budget` | The child exited 2. The Python loop stayed silent past the ping budget. |
| `killed` | `sigkill` | `SIGKILL` arrived and this supervisor did not send it. |
| `killed` | `crash` | Any other exit code or signal. |
| `killed` | empty | The metadata hash is still there, the worker heartbeat is stale, and no live supervisor record exists. The supervisor itself is gone, including when `memory.oom.group` kills the whole container. |

`killed` with no reason is not `killed` / `memory_hard`. `memory_hard` means this supervisor recorded the signal it sent. An empty reason means there is no supervisor left to record one. A row that is `killed` and still lists a task is waiting for a live neighbor to requeue it.
