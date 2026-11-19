# Tracing and the admin board

These pages describe simple-q on main. The packages are 1.3.0. Changes after that tag are under Unreleased in the changelog.

## Tracing

The child does not open an OTLP connection. It reads `tracing` on the loaded application. `None`, or a missing attribute, installs no OpenTelemetry layer and does not call into Python to export spans.

```python
from opentelemetry.sdk.resources import Resource

from simple_q import App, Client, Config, Tracing, actor


@actor(queue="default")
async def render_report(report_id: int) -> None:
    ...


def create_app() -> App:
    config = Config.from_env()
    app = App(client=Client("redis://redis:6379/0", config=config), actors=[render_report])
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

## Supervisor telemetry

The supervisor does not use the application's `Tracing` exporter. That path needs the interpreter, and the supervisor does not start one. Supervisor traces and Sentry are a separate Rust export. They stay off until an OTLP endpoint or a Sentry DSN is set. The variable table is on the [settings](settings.md) page.

Spans cover supervisor start and stop, child start, child exit, a restart pause, and a drain. A child exit records the code or the signal and a reason: `recycle`, `drain`, `memory_soft`, `memory_ratio`, `hard kill`, or `crash`. Those spans are not linked to task spans. The supervisor does not run tasks.

Sentry records a supervisor panic, a restart pause that has reached its 30 second cap, a child that did not start, a hard kill, and a supervisor Redis publish that fails for longer than the heartbeat window. The DSN and OTLP headers are read from the environment only. They are not command-line flags, and they are not written to the log, to `agent-worker info`, to the supervisor Redis record, or to span attributes. `info` shows `on` or `off`.

```bash
SIMPLE_Q_SUPERVISOR_OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
SIMPLE_Q_SUPERVISOR_SENTRY_DSN=https://public@sentry.example/1 \
agent-worker worker --app reports_app:create --factory --queues reports \
  --otel-service-name simple-q-supervisor
```

The `SIMPLE_Q_SUPERVISOR_` endpoint wins over `OTEL_EXPORTER_OTLP_ENDPOINT`. The child still inherits `OTEL_EXPORTER_OTLP_ENDPOINT` unchanged, for the application's own SDK. A collector that is down drops spans. The supervisor does not exit because of that.

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
| `killed` | `ping_budget` | The child exited 2. The Python loop stayed silent past the ping budget. Exit 2 is not used for a Redis error. |
| `killed` | `ownership_lost` | The child exited 4. The attempt lost its lease, or finish was not confirmed. The message stays inflight. |
| `killed` | `redis_error` | The child exited 5. Redis failed while counting the delivery or enqueueing a retry. The attempt stays inflight. |
| `killed` | `sigkill` | `SIGKILL` arrived and this supervisor did not send it. |
| `killed` | `crash` | Any other exit code or signal. |
| `killed` | empty | The metadata hash is still there, the worker heartbeat is stale, and no live supervisor record exists. The supervisor itself is gone, including when `memory.oom.group` kills the whole container. |

`killed` with no reason is not `killed` / `memory_hard`. `memory_hard` means this supervisor recorded the signal it sent. An empty reason means there is no supervisor left to record one. A row that is `killed` and still lists a task is waiting for a live neighbor to requeue it.

## Supervisor binary

The release binary is unstripped. Sizes are `target/release/agent-worker` on x86_64.

| Build | Bytes |
| --- | --- |
| 1.3.0 (`b6a05ee`) | 19,352,888 |
| This fork with `grpc-tonic` and rustls `aws-lc` linked | 30,364,616 |
| 2.0.0a1 default: OTLP `http/protobuf`, rustls `ring` | 26,912,368 |

The default build is 3,452,248 bytes smaller than the same fork with gRPC and `aws-lc` linked. `aws-lc-rs` and `aws-lc-sys` are not in `Cargo.lock`. The wheels use this default. `otlp-grpc` is off unless the binary is rebuilt with that cargo feature. `Cargo.lock` went from 265 packages at 1.3.0 to 366. The gRPC and `aws-lc` packages that left this fork's lock are `aws-lc-rs`, `aws-lc-sys`, `cmake`, `dunce`, `fs_extra`, `lru-slab`, `quinn`, `quinn-proto`, `quinn-udp`, `rand_pcg`, `rustc-hash`, and `tinyvec`. `tonic` stays in the lock so `--features otlp-grpc` still builds with `--locked`, and it is not linked into the default binary.
