# Architecture diagrams (Stage B target)

Baseline contrast: today the parent initializes CPython to read the App; target forbids that.

## Target context

```mermaid
C4Context
  title Target context (fork)
  System_Ext(redis, "Redis", "Queue ownership, heartbeats, idempotency, admin records")
  System_Ext(os, "OS", "Processes, signals, cgroups")
  Person(app_dev, "Application", "Python handlers, DI, FSM")
  Person(ops, "Operator", "info / QueueAdmin")
  System_Boundary(sq, "simple-q wheel") {
    System(parent, "Rust parent", "ProcessPort + policy; no CPython")
    System(child, "Rust child + queue runtime", "QueuePort + drain barrier")
    System(py, "CPython child side", "Bridge + App")
  }
  Rel(app_dev, py, "handlers / hooks")
  Rel(ops, child, "admin reads")
  Rel(parent, os, "spawn/signal/wait")
  Rel(parent, child, "supervise children")
  Rel(child, redis, "claim/renew/ack")
  Rel(child, py, "dispatch / outcomes")
  Rel(py, redis, "business lock via runtime API")
```

## Ports & adapters

```mermaid
flowchart LR
  subgraph core [crates/core]
    SM[State machines]
    Pol[Policy]
    QP[QueuePort]
    PP[ProcessPort]
    BP[BridgePort]
  end
  subgraph adapters [adapters]
    Apalis[backend/apalis]
    Wx[supervisor/watchexec]
    PyO3[python-bridge]
  end
  CLI[cli composition root] --> core
  CLI --> adapters
  Apalis -.implements.-> QP
  Wx -.implements.-> PP
  PyO3 -.implements.-> BP
  SM --> QP
  SM --> PP
  SM --> BP
  Pol --> PP
```

## Drain barrier (Tokio notify → wait)

```mermaid
sequenceDiagram
  participant Adm as Admission
  participant Tr as TaskTracker
  participant Q as QueuePort
  participant B as Bridge
  Note over Adm,Tr: discover shutdown
  Adm->>Adm: close admission
  Adm->>Tr: close tracker (no new tasks)
  par in-flight attempts
    Q-->>Tr: Finishing ACK/retry still runs
    B-->>Tr: handler may still finish
  end
  Tr->>Tr: wait registered tasks
  Note over Adm,Tr: then child exit → parent reap → escalate budgets
```

## Attempt lifecycle

```mermaid
stateDiagram-v2
  [*] --> Claimed
  Claimed --> HandlerRunning
  HandlerRunning --> HandlerFinished: bridge success/error
  HandlerRunning --> LeaseLost
  HandlerRunning --> Cancelled
  HandlerRunning --> BridgeFailed
  HandlerFinished --> Finishing: ack/retry/dead
  Finishing --> Terminal
  LeaseLost --> Terminal
  Cancelled --> Terminal
  BridgeFailed --> Terminal
```
