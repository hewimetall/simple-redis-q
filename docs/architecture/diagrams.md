# Architecture diagrams (Stage B binding)

These diagrams are the target. The process that ships is [architecture.md](../architecture.md). The parent on main still starts CPython and still reads the Redis URL from `App.client`.

## Attempt lifecycle (axes)

```mermaid
stateDiagram-v2
  [*] --> Claimed
  Claimed --> Running: StartHandler
  Running --> CancelRequested: RequestCancel
  Running --> HandlerFinished: HandlerReturned
  CancelRequested --> HandlerFinished: HandlerReturned
  CancelRequested --> Stopped: ConfirmExecutionStopped
  note right of Running
    LoseOwnership sets ownership=Lost
    and does not change execution by itself.
    HandlerReturned keeps ownership=Lost.
  end note
  HandlerFinished --> Finishing: BeginFinish
  Stopped --> Finishing: BeginFinish
  Finishing --> Terminal: Confirmed / RejectedStale / Unconfirmed
```

## Drain barrier (admission ≠ TaskTracker.close)

```mermaid
sequenceDiagram
  participant Gate as AdmissionGate
  participant Backend as Queue adapter
  participant Tr as TaskTracker
  Note over Gate,Tr: discover shutdown
  Gate->>Gate: close admission
  Backend->>Backend: pause/stop fetch loop
  Note over Gate: settle in-flight claim race; register Attempts
  Gate->>Gate: mark_inflight_settled
  Note over Tr: close does not block spawn; it allows wait to finish when empty
  Tr->>Tr: tracker.close()
  Tr->>Tr: tracker.wait()
  Note over Gate,Tr: hooks → child exit → parent reap → escalate
```

## Target context

```mermaid
C4Context
  title Target context (fork)
  System_Ext(redis, "Redis", "Queue ownership, heartbeats, idempotency, admin records")
  System_Ext(os, "OS", "Processes, signals, cgroups")
  Person(app_dev, "Application", "Python handlers, DI, FSM")
  System_Boundary(sq, "pip install simple-q-worker") {
    System(parent, "Rust parent", "ProcessPort + policy; no CPython")
    System(child, "Rust child + queue runtime", "backend-owned fetch")
    System(py, "CPython child", "Bridge + App.client")
  }
  Rel(app_dev, py, "handlers / hooks")
  Rel(parent, os, "spawn/signal/wait")
  Rel(parent, child, "supervise")
  Rel(child, redis, "claim/renew/finish via adapter")
  Rel(child, py, "dispatch / outcomes")
```
