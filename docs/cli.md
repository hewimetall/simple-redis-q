# Command line

These pages describe simple-q 1.2. The binary is `agent-worker`, installed by `simple-q-worker`.

`--app` is required wherever a worker or a one-shot task needs the application. The value is `module:attribute`.

## `worker`

```bash
agent-worker worker --app module:create_app --factory --queues default
agent-worker worker --app module:app --queues default
```

`--factory` calls the attribute and uses the returned `App`. Without `--factory`, the attribute is the `App`.

The process without the internal child flag only supervises. It forks one child per slot. The child embeds the CPython minor the wheel was linked against (3.12, 3.13, 3.14, or 3.15, with the GIL), imports the application, and runs the queues.

| Flag | Default | Role |
| --- | --- | --- |
| `--app` | required | `module:attribute` |
| `--factory` | off | Call the attribute to build the `App` |
| `--queues` | `default` | Comma-separated queue names. Each name is one lowercase ASCII word |
| `--processes` | `1` | Child processes. Ignored for the internal child process |
| `--concurrency` | `1` | In-flight tasks in one child |
| `--max-tasks-per-process` | `500` | Completed tasks after which the child drains and is replaced |
| `--child-memory-soft` | unset | Soft RSS for one child, such as `512K`, `1500M`, or `1G` |
| `--child-memory-hard` | unset | Hard RSS for one child |

`K`, `M`, and `G` are powers of 1024. A bare number is bytes. The flag wins over `SIMPLE_Q_CHILD_MEMORY_SOFT` or `SIMPLE_Q_CHILD_MEMORY_HARD`. An invalid limit exits 3 before a child starts.

`--child` is internal. The supervisor starts the embedded interpreter only in that mode.

The supervisor also starts the metrics scrape. The worker command does not read `REDIS_URL`. It connects with the URL on the `App`'s `Client`. `enqueue` and `admin` do read `REDIS_URL`. `info` uses the same client URL as `worker` and does not write a heartbeat.

`--app` may be omitted when `SIMPLE_Q_APP` is set. `SIMPLE_Q_APP_FACTORY` is the same as `--factory` when its value is `1`, `true`, `yes`, or `on`. `false`, `0`, `no`, `off`, and an empty value leave the flag off.

## `info`

```bash
agent-worker info
agent-worker info --queue quota --json
agent-worker info --all
```

Read-only. The command loads the application only to take the Redis URL and `Config` from its `Client`. It does not register a worker and it does not heartbeat.

By default it prints the supervisor on this host (`HOSTNAME`, the same value as the worker name), that supervisor's processes and the tasks they hold, and the counters for that supervisor's queues. `--all` prints every supervisor and the orphan processes. `--queue` may be repeated and limits the queue counters. A process still lists held tasks for every queue on its supervisor record, including a queue that was not passed. `--json` prints the same object `QueueAdmin.snapshot` returns.

Exit 0. Exit 1 when `--all` is absent and this host has no live supervisor. The message is `no live supervisor on {host}` on stderr.

```text
supervisor  worker-quota-6f8b9c7d4-x2k9p  pid 1  up 38m  simple-q 1.2.0
queues      quota  processes 3/3  concurrency 1  max_tasks 500
memory      cgroup 2.1G / 4G  child soft 1G / hard 1500M
counters    recycled 14  soft 2  hard 1  restarts 0
SLOT  PID  STATE     REASON       RSS    TASKS    HB
0     31   running                812M   412/500  4s
1     57   draining  max_tasks    640M   500/500  9s
2     44   draining  memory_soft  1.02G  212/500  3s
0     23   killed    memory_hard  1.51G  98/500   41s   held 1, waiting for requeue
1     12   exited    max_tasks    -      500/500  52s
PID  QUEUE  ACTOR                        KWARGS                  TRY  DLV  RUN
31   quota  recalc                       project_id=3f1c0b2a…    0/1  1    212s/1800s
23   quota  recalc                       project_id=held         0/1  1    212s/1800s
QUEUE  ACTIVE  SCHEDULED  INFLIGHT  DONE  DEAD
quota  12      1          4         9873  0
```

`processes 3/3` is live children over the configured count. `soft` and `hard` in the counters line are soft drains and hard kills since this supervisor started. A killed or exited row that still holds a task says it is waiting for requeue. `simple-q 1.2.0` is the package version. A pre-release still prints the Cargo form on `--version` (`1.0.0-rc7`) and the PEP 440 form on this line (`1.0.0rc7`).

## `admin`

```bash
agent-worker admin
```

Serves the admin board. It does not run a worker and it does not load an `App`. The Redis URL is `REDIS_URL`. `QUEUE` selects the queue and defaults to `default`. Details are on the [tracing and admin](operations.md) page.

## `metrics`

```bash
agent-worker metrics
```

Serves the multiprocess Prometheus scrape. The supervisor starts this itself for `worker`. Run it directly only when the scrape should live in its own process. The listen address is `DRAMATIQ_PROM_HOST`:`DRAMATIQ_PROM_PORT`, default `0.0.0.0:9191`. Any path returns the scrape.

## `enqueue`

```bash
agent-worker enqueue --queue default --task recalc --payload '{"project_id": "42"}'
```

Enqueue one JSON object. The command reads `REDIS_URL` and `Config.from_env()`. It does not load an `App`. It fails immediately when Redis is unavailable. `--headers` is a JSON object of strings. `--max-retries` defaults to 0. Backoff defaults are 15000 ms and 7 days. The time limit stored on this path is 30 minutes. This command does not set a lock or a unique key. Application code should use `Client.send_with_options` for `unique_key="recalc:42"`.

## `execute`

```bash
agent-worker execute --app module:app --task recalc --task-id recalc-1
```

Run one task in-process, without Redis. `--app` is `module:attribute`.

## `agent-worker --version`

Prints the worker crate version. The worker package depends on that exact `simple-q-runtime` version.
