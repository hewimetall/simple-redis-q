# Settings

These pages describe simple-q 1.0.

`Config.from_env()` reads `SIMPLE_Q_*` only. An invalid value raises. There is no silent default: a value that is empty, not a number, zero where the setting must be positive, or a ratio outside `(0, 1)` raises. An omitted variable uses the default in this table. The Redis URL is not one of these settings. The application passes it to `Client`. `agent-worker enqueue` and `agent-worker admin` read `REDIS_URL` themselves.

`dramatiq_*` metric names and labels are a stable API until 2.0. The names are `dramatiq_messages_total`, `dramatiq_message_errors_total`, `dramatiq_message_retries_total`, `dramatiq_message_rejects_total`, `dramatiq_messages_inprogress`, `dramatiq_delayed_messages_inprogress`, and `dramatiq_message_duration_milliseconds`. The labels are `queue_name` and `actor_name`. `queue_name` is canonical: a `.DQ` or `.XQ` suffix is removed. Both gauges use `livesum`. A retry increments `dramatiq_message_retries_total` once, on the canonical queue, when the retry is enqueued with a delay. Messages waiting in Redis are in Redis, so they do not move `dramatiq_delayed_messages_inprogress`. That gauge moves only while this process holds the message, and it returns to zero when this process releases it.

A future `Config(metrics_prefix=...)` is not implemented.

The child-memory counters `simple_q_child_memory_drains_total` and `simple_q_child_memory_kills_total` are separate samples written by the supervisor.

`--max-tasks-per-process` defaults to 500. That limit is a flag, not an environment variable. The ping budget defaults to the actor time limit plus 60 seconds. The default time limit is 1_800_000 ms, so an omitted ping budget is 1860 seconds.

## `SIMPLE_Q_*`

| Variable | Default | Meaning |
| --- | --- | --- |
| `SIMPLE_Q_HEARTBEAT_SECS` | `15` | Seconds between worker heartbeats. Must be a positive integer. |
| `SIMPLE_Q_MISSED_HEARTBEATS` | `4` | Missed heartbeats before a worker is not alive. The window is this value times the heartbeat. The default window is 60 seconds. |
| `SIMPLE_Q_PING_BUDGET_SECS` | time limit + 60s | How long the Python loop may stay silent before the child exits 2. The default time limit is 30 minutes, so the omitted budget is 1860 seconds. A positive integer replaces that budget. |
| `SIMPLE_Q_UNIQUE_TTL_SECS` | `86400` | How long a first send may hold `unique_key`. Must be a positive integer. Zero is not a TTL. |
| `SIMPLE_Q_MAX_DELIVERIES` | `5` | Poison when `deliveries` is greater than this. The delivery that lands on the limit still runs. Must be a positive integer. |
| `SIMPLE_Q_DONE_TTL_SECS` | `86400` | Age after which a successful record is deleted by the sweeper. 24 hours. |
| `SIMPLE_Q_DEAD_TTL_SECS` | `604800` | Age after which a dead record is deleted by the sweeper. 7 days. |
| `SIMPLE_Q_CHILD_MEMORY_SOFT` | unset | Soft RSS for one child. Used when `--child-memory-soft` is omitted. The flag wins when both are set. |
| `SIMPLE_Q_CHILD_MEMORY_HARD` | unset | Hard RSS for one child. Used when `--child-memory-hard` is omitted. The flag wins when both are set. |
| `SIMPLE_Q_MEMORY_SOFT_RATIO` | `0.80` | Working-set fraction that drains the largest child. Finite, and inside `(0, 1)`. |
| `SIMPLE_Q_MEMORY_HARD_RATIO` | `0.95` | Working-set fraction that `SIGKILL`s the largest child when that child owns at least half the usage. Finite, and inside `(0, 1)`. |
| `SIMPLE_Q_SLOT` | set by the supervisor | Metric-file id for one child. The supervisor sets it. Files are named by this slot, not by the operating-system pid. |

Child memory suffixes are `K`, `M`, and `G` as powers of 1024, with an optional `B`. A bare number is bytes. Soft must be below hard when both are set.

## `DRAMATIQ_PROM_*`

The scrape also accepts the lowercase aliases `dramatiq_prom_host`, `dramatiq_prom_port`, and `dramatiq_prom_db`. `PROMETHEUS_MULTIPROC_DIR` and `prometheus_multiproc_dir` are directory aliases for the same multiprocess path.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DRAMATIQ_PROM_HOST` | `0.0.0.0` | Scrape bind address. |
| `DRAMATIQ_PROM_PORT` | `9191` | Scrape port. A non-integer raises. |
| `DRAMATIQ_PROM_DB` | temp dir `dramatiq-prometheus` | Multiprocess directory. The parent wipes `*.db` once. Each child writes files under `SIMPLE_Q_SLOT`. The same directory holds `simple_q_child_memory.prom`. |

Any HTTP path on that address returns the scrape. A missing `simple_q_child_memory.prom` contributes zero for both child-memory counters.

## Read by other code, not by `Config.from_env`

`Config.from_env` does not consult this table. `admin` reads `QUEUE` and `BOARD_*`. The worker name reads `HOSTNAME`. The text log reads `RUST_LOG`. The metrics process reads `DRAMATIQ_PROM_*`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `QUEUE` | `default` | Queue shown by `agent-worker admin`. |
| `BOARD_ADDR` | `127.0.0.1:8080` | `ip:port` for the admin board. |
| `BOARD_TOKEN` | required, except the insecure loopback case | At least 16 characters from `A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, `-`. |
| `BOARD_INSECURE` | unset | `1` allows a loopback listener to omit the token. Any other address still requires the token. |
| `RUST_LOG` | `info` | Text log filter. It does not filter the OpenTelemetry layer. |
| `HOSTNAME` | `/etc/hostname`, then `localhost` | Host field inside the worker name and the supervisor record. |
| `SIMPLE_Q_APP` | unset | `module:attribute` for `worker` and `info` when `--app` is omitted. |
| `SIMPLE_Q_APP_FACTORY` | unset | `1`, `true`, `yes`, or `on` makes `worker` and `info` call the attribute, the same as `--factory`. `false`, `0`, `no`, `off`, and empty do not. |
