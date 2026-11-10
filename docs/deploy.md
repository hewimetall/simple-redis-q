# Deploy

These pages describe simple-q 1.0.

Install `simple-q-runtime` and `simple-q-worker` on CPython 3.12, Linux x86_64 or aarch64. The supervisor process is `agent-worker`. It forks children. Give the service a finite memory limit so the cgroup ratio backstop has a `memory.max` to read. Per-child RSS limits are optional and apply even when the ratio check is idle.

The worker command in every example is:

```bash
agent-worker worker --app module:create_app --factory --queues default
```

`module:app` is the same command without `--factory`, when the attribute is already an `App`. `--app` is required. The factory passes the Redis URL to `Client`. The library does not read `REDIS_URL`.

## How OOM differs

The supervisor checks the cgroup about once a second. Usage is `memory.current` minus `inactive_file`. At `SIMPLE_Q_MEMORY_HARD_RATIO` (0.95) it `SIGKILL`s the largest child when that child's RSS is at least half the usage. That kill is deliberate: the supervisor must `SIGKILL` the child first.

A cgroup with `memory.oom.group` kills the whole container. Every process in the cgroup dies, including the supervisor, so the supervisor cannot start a replacement child. Kubernetes sets that grouping on a container cgroup. Set a memory limit, keep the hard ratio at 0.95, and let the supervisor kill the child before the kernel OOM runs.

systemd and Docker do not do that grouping unless configured. systemd's `MemoryOOMGroup=` defaults off, so a kernel OOM kills one process in the unit. Docker does not set `memory.oom.group` unless the cgroup was configured that way. The ratio backstop still runs whenever `memory.max` is finite, on all three.

`SIGKILL` from the supervisor restarts that child. A kernel OOM that kills the supervisor does not.

## Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: simple-q
spec:
  replicas: 1
  selector:
    matchLabels:
      app: simple-q
  template:
    metadata:
      labels:
        app: simple-q
    spec:
      containers:
        - name: worker
          image: simple-q:1.0
          args:
            - worker
            - --app
            - module:create_app
            - --factory
            - --queues
            - default
          env:
            - name: SIMPLE_Q_CHILD_MEMORY_SOFT
              value: 512M
            - name: SIMPLE_Q_CHILD_MEMORY_HARD
              value: 1G
          ports:
            - name: metrics
              containerPort: 9191
          resources:
            limits:
              memory: 2Gi
          readinessProbe:
            httpGet:
              path: /metrics
              port: metrics
```

`resources.limits.memory` becomes `memory.max`. Because the container cgroup uses `memory.oom.group`, an OOM kill ends the whole container. The hard ratio exists so the supervisor `SIGKILL`s the child at 95 percent of that limit, while the supervisor is still alive to replace it. If `processes * hard` is larger than `memory.max`, the supervisor logs a warning and continues.

Scrape port 9191 on any path. The admin board is a separate process and should stay on loopback. See [Tracing and the admin board](operations.md).

## Docker

The repository `Dockerfile` builds `agent-worker` on `python:3.12-slim-bookworm` and installs the extension wheel. The image entrypoint is `agent-worker`.

```bash
docker build -t simple-q:1.0 .
docker run --memory=2g \
  -p 127.0.0.1:9191:9191 \
  simple-q:1.0 \
  worker --app module:create_app --factory --queues default
```

`--memory` sets a finite `memory.max`, so the ratio backstop runs. Docker leaves `memory.oom.group` unset unless the cgroup was given that file. A kernel OOM then kills one process. The supervisor still `SIGKILL`s the largest child at the hard ratio so a single child does not have to wait for that kernel choice.

Redis published on a host port has the same trust boundary as enqueue. Set a password before sharing that port.

## systemd

```ini
[Service]
User=simple-q
ExecStart=/opt/simple-q/.venv/bin/agent-worker worker --app module:create_app --factory --queues default
Environment=DRAMATIQ_PROM_HOST=0.0.0.0
Environment=DRAMATIQ_PROM_PORT=9191
MemoryMax=2G
Restart=on-failure
RestartSec=2
```

`MemoryMax=` gives the service a finite `memory.max`. Leave `MemoryOOMGroup=` unset. systemd does not group the OOM kill unless that setting is turned on. With the default, a kernel OOM kills one process in the cgroup. The supervisor's hard ratio still `SIGKILL`s the child first when the working set crosses 0.95.

`Restart=on-failure` restarts the supervisor when the supervisor itself exits. Child exit 2 and a child `SIGKILL` are handled inside the supervisor and do not need a unit restart. Exit 3 is a bad memory limit or a bad tracing configuration. The supervisor stops. Fix the configuration before starting the unit again.
