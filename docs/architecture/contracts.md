# Executable contracts — Attempt, QueuePort, drain

**Status:** Stage B (binding). Removes the `or` ambiguity from the earlier plan draft.  
**Code:** `crates/simple-q-core` (types + AAA unit tests).

## 1. Who owns the fetch loop

**Decision: the queue backend adapter owns the fetch/claim loop** (Apalis `WorkerBuilder` today).

Core does **not** call `claim()` in a polling loop. The adapter delivers an `Attempt` into core/runtime when a message is already owned.

Rationale: matches Apalis 1.1.1; avoids re-implementing Redis claim; keeps Oxana comparison honest (Oxana `dispatcher` also owns claim via LMOVE — see spikes).

## 2. `Attempt`

An `Attempt` is one owned delivery:

| Field | Meaning |
| --- | --- |
| `attempt_id` | Correlation / fencing id for this claim |
| `message_id` | Stable across retries |
| `worker_id` | Worker that holds the lease |
| `ownership` | `OwnershipToken` required on finish |
| `job` | Opaque payload (JSON `Job` today) |

### State machine (binding)

```text
Claimed
  → HandlerRunning
      → CancelRequested          # request only; Python may still run
      → OwnershipLostWhileRunning # lease gone; Python may still run
      → HandlerFinished
  → StoppingExecution            # execution stop confirmed
  → HandlerFinished
      → Finishing                # ack/retry/dead/reject in flight
          → Terminal             # only after finish confirmed|rejected|unconfirmed
```

**Forbidden:** `LeaseLost → Terminal`, `CancelRequested → Terminal`, `BridgeFailed → Terminal` without `Finishing`.

Cancel **request** ≠ cancel **confirmed**. Ownership loss while running is a first-class state.

## 3. Finish / lease-loss results

| Result | Meaning |
| --- | --- |
| `Ack` | Backend accepted success finish with valid ownership |
| `Retry { delay }` | New delayed delivery enqueued; old ownership released atomically w.r.t. adapter contract |
| `Dead { reason }` | Terminal failure recorded |
| `RejectedStaleOwnership` | Finish refused; another claim owns the message |
| `FinishUnconfirmed` | Transport uncertainty; must not pretend ACK succeeded |

Retry relative to old delivery: adapter must not leave two live inflight ownerships for the same attempt token. Proven by contract tests (Stage C/D), not by docs alone.

## 4. QueuePort (narrow)

```text
QueuePort
  // Producer side (may be shared with Python extension adapter)
  enqueue(job, delay?) -> MessageId | DuplicateUniqueKey

  // Worker side: backend-owned loop calls into runtime with Attempt
  // Port surface for finish + observation:
  finish(attempt_id, ownership, disposition) -> FinishDisposition
  // Lease renewal is backend-driven (Apalis heartbeat).
  // Runtime observes loss via BridgeOutcome::LeaseLost / AttemptEvent::LoseOwnership
  // sourced from adapter callbacks or orphan detection — never assumed from sleep.
```

No `claim()` on the core port. No “start worker loop hooks **or** stream” duality.

Admin reads use a segregated `AdminPort` (existing snapshot needs) — not part of finish.

## 5. Drain barrier

Ordered phases (`DrainPhase` in core):

1. **AdmissionClosed** — stop accepting new Attempts (pause/stop Apalis workers / close gate).  
2. Resolve in-flight claim race via `AdmissionGate` (default: accept-and-track).  
3. **WaitingTrackedWork** — `TaskTracker::wait` (or equivalent) for registered handler+finish tasks.  
4. Hooks → child exit → parent reap → escalate.

**Correction:** `TaskTracker::close` does **not** by itself close Redis admission. Diagrams must show two steps: close admission gate, then close tracker / wait.

## 6. Bridge outcomes

Closed enum `BridgeOutcome` in core. Unknown JSON `outcome` string → `BridgeFailure` / protocol error, never success.

## 7. Backlog (ports must not forbid)

Tracked for later stages; first vertical slice may stub:

- Object Python bridge (dataclass / typed PyO3), not only JSON dict  
- Per-message and per-actor errbacks  
- Admin cancel/restart bound to a specific Python `execution_id`  
- Batch claim → inflight SET → local buffer  
- Batch renewal and batch finish  

`AttemptId` / `OwnershipToken` / cancel-request vs confirm exist so these can attach without rewriting the state machine.
