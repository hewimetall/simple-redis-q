//! Executable port types. Semantics: `docs/architecture/contracts.md`.

use std::time::Duration;

/// Stable id of a queue message across retries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueMessageId(pub String);

/// Id of one claim/execution attempt (fencing / correlation).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttemptId(pub String);

/// Worker identity that holds the lease for this attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkerId(pub String);

/// Opaque ownership proof required for finish. Adapter maps backend tokens here.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnershipToken(pub String);

/// Closed bridge outcomes. Unknown wire values are protocol errors at the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeOutcome {
    Ok,
    Retry {
        delay: Duration,
    },
    Dead {
        reason: String,
    },
    Skipped,
    ApplicationError {
        message: String,
    },
    Timeout,
    /// Cancel was requested and Python confirmed stop.
    CancelConfirmed,
    /// Lease/ownership lost while work may still be running locally.
    LeaseLost,
    BridgeFailure {
        message: String,
    },
}

/// Backend disposition after local execution ends (or is abandoned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishDisposition {
    Ack,
    Retry {
        delay: Duration,
    },
    Dead {
        reason: String,
    },
    /// Ownership no longer valid; must not ack/retry as this attempt.
    RejectedStaleOwnership,
    /// Finish could not be confirmed (transport). Local attempt still recorded.
    FinishUnconfirmed {
        detail: String,
    },
}
