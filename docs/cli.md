# Command line

These pages describe simple-q 1.0. The binary is `agent-worker`, installed by `simple-q-worker`.

`--app` is required wherever a worker or a one-shot task needs the application. The value is `module:attribute`.

## `worker`

```bash
agent-worker worker --app module:create_app --factory --queues default
agent-worker worker --app module:app --queues default
```

`--factory` calls the attribute and uses the returned `App`. Without `--factory`, the attribute is the `App`.

The process without the internal child flag only supervises. It forks one child per slot. The child embeds CPython 3.12, imports the application, and runs the queues.

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

The supervisor also starts the metrics scrape. The Redis URL is the one on the `App`'s `Client`. The library does not read `REDIS_URL`.

## `admin`

```bash
agent-worker admin
```

Serves the admin board. It does not run a worker. The Redis URL is the one the application passed to `Client`. `QUEUE` selects the queue and defaults to `default`. Details are on the [tracing and admin](operations.md) page.

## `metrics`

```bash
agent-worker metrics
```

Serves the multiprocess Prometheus scrape. The supervisor starts this itself for `worker`. Run it directly only when the scrape should live in its own process. The listen address is `DRAMATIQ_PROM_HOST`:`DRAMATIQ_PROM_PORT`, default `0.0.0.0:9191`. Any path returns the scrape.

## `enqueue`

```bash
agent-worker enqueue --queue default --task recalc --payload '{"project_id": "42"}'
```

Enqueue one JSON object. Fails immediately when Redis is unavailable. `--headers` is a JSON object of strings. `--max-retries` defaults to 0. Backoff defaults are 15000 ms and 7 days. The time limit stored on this path is 30 minutes. This command does not set a lock or a unique key. Application code should use `Client.send_with_options` for `unique_key="recalc:42"`.

## `execute`

```bash
agent-worker execute --app module:app --task recalc --task-id recalc-1
```

Run one task in-process, without Redis. `--app` is `module:attribute`.

## `agent-worker --version`

Prints the worker crate version. The worker package depends on that exact `simple-q-runtime` version.
