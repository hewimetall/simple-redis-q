# agent-worker

Rust supervisor and embedded CPython 3.12 runtime for registered Python async tasks.

The binary is `agent-worker`. The importable Python package is `simple_q`. The Rust module, embedded in the worker and also built as a wheel, is `simple_q_runtime`.

Importing `simple_q` does not open Redis. Call `simple_q.configure(redis_url=...)` after the process is the one that will send, including after a gunicorn fork. `send` before that call fails.

Product queues are `default` and `quota`. The decision for replacing Dramatiq is recorded in [docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md](docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md).

Metrics keep the Dramatiq names and the accounting of the fixed Prometheus middleware: canonical `queue_name`, both gauges in `livesum`, and a retry counted once when it is enqueued with a delay. Redis-scheduled retries are not in-process memory, so they do not move `dramatiq_delayed_messages_inprogress`. The scrape listens on `DRAMATIQ_PROM_HOST`:`DRAMATIQ_PROM_PORT` (default `0.0.0.0:9191`) and any path. `DRAMATIQ_PROM_DB` is the multiprocess directory. The supervisor wipes `*.db` once; each child writes files under `SIMPLE_Q_SLOT`.

`agent-worker admin` listens on `127.0.0.1:8080`. Set `BOARD_ADDR` only together with `BOARD_TOKEN` (at least 16 letters or digits). Open the board once as `/?token=...`; the token is then kept in an HttpOnly cookie. The compose file publishes that port on the host loopback and still requires `BOARD_TOKEN`, because the process inside the container listens on the container network.
