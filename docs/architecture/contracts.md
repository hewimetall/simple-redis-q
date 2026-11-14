# Executable contracts — Attempt, QueuePort, drain

**Status:** Stage B (binding).  
**Code:** `crates/simple-q-core` (types + AAA unit tests).

## 1. Who owns the fetch loop

**Decision: the queue backend adapter owns the fetch/claim loop** (Apalis `WorkerBuilder` today).

Core does **not** call `claim()` in a polling loop. The adapter delivers work into core/runtime when a message is already owned.

## 2. `Attempt` lifecycle (three axes)

An attempt carries correlation ids (`attempt_id`, `message_id`, `worker_id`, `OwnershipToken`, job payload) and three independent axes:

| Axis | Values |
| --- | --- |
| `execution` | `Claimed` → `Running` → `CancelRequested` / `Stopped` / `HandlerFinished` |
| `ownership` | `Held` \| `Lost` |
| `finish` | `NotStarted` → `InFlight` → `Confirmed` \| `RejectedStale` \| `Unconfirmed` |

`HandlerReturned` must **not** clear `ownership = Lost`.  
`allowed_success_ack()` is true only when `ownership == Held` and `finish == InFlight`.

```text
execution:
  Claimed → Running                 # StartHandler only
  Running → CancelRequested         # RequestCancel
  Running | CancelRequested → HandlerFinished   # HandlerReturned
  CancelRequested → Stopped         # ConfirmExecutionStopped
  Running (ownership Lost) → Stopped # ConfirmExecutionStopped after lease loss
  HandlerFinished | Stopped → (finish InFlight)

ownership:
  Held → Lost                       # LoseOwnership (idempotent until terminal)
  # never reset by HandlerReturned

finish:
  NotStarted → InFlight             # BeginFinish
  InFlight → Confirmed | RejectedStale | Unconfirmed
```

`ExecutionState::Stopped` means stop is **already confirmed** — next step is `BeginFinish`, not another wait for Python.

Source of truth: `AttemptLifecycle::transition` in `simple_q_core::attempt`.

## 3. Finish / lease-loss results

| Result | Meaning |
| --- | --- |
| `Ack` | Backend accepted success finish with valid ownership |
| `Retry { delay }` | New delayed delivery; old ownership released per adapter contract |
| `Dead { reason }` | Terminal failure recorded |
| `RejectedStaleOwnership` | Finish refused; another claim owns the message |
| `FinishUnconfirmed` | Transport uncertainty; must not pretend ACK succeeded |

## 4. QueuePort (narrow)

```text
QueuePort
  enqueue(job, delay?) -> MessageId | DuplicateUniqueKey
  finish(attempt_id, ownership, disposition) -> FinishDisposition
  # Lease renewal is backend-driven.
  # Loss → AttemptEvent::LoseOwnership / BridgeOutcome::LeaseLost
```

No core `claim()` poll loop. Admin reads use segregated `AdminPort`.

## 5. Drain barrier

Tokio `TaskTracker::close` does **not** forbid new task registration. It allows `wait()` to finish when the tracker is empty.

Binding order:

```text
close admission
→ settle in-flight claims (AdmissionGate::mark_inflight_settled)
→ register every accepted Attempt
→ tracker.close()
→ tracker.wait()
```

Race to avoid: `close`+empty `wait` completing while a late claim can still register work. After `mark_inflight_settled`, further claims are `Reject`.

## 6. Bridge outcomes

Closed enum `BridgeOutcome`. Unknown JSON `outcome` → protocol error, never success.

## 7. Backlog (ports must not forbid)

- Object Python bridge; per-message/per-actor errbacks  
- Admin cancel/restart bound to `execution_id`  
- Batch claim / renewal / finish  
- Unresponsive Python / nested `Python::attach` hang (out of this PR’s production fix scope)
