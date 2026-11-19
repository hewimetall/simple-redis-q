//! Queue port types. Semantics: `docs/architecture/contracts.md`.

use std::time::Duration;

/// Stable id of a queue message across retries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueMessageId(pub String);

/// Id of one claim/execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttemptId(pub String);

/// Worker identity that holds the lease for this attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkerId(pub String);

/// Opaque ownership proof required for finish.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnershipToken(pub String);

/// Backend disposition after local execution ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishDisposition {
    Ack,
    Retry {
        delay: Duration,
    },
    Dead {
        reason: String,
    },
    /// Ownership no longer valid; must not ack or retry as this attempt.
    RejectedStaleOwnership,
    /// Finish could not be confirmed. Do not treat it as an ack.
    FinishUnconfirmed {
        detail: String,
    },
}

/// A duplicate `unique_key` is not a transport failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueError {
    DuplicateUniqueKey,
}

/// Narrow queue port. The adapter owns the fetch loop.
///
/// Lease renewal stays inside the backend. Loss is reported as
/// `AttemptEvent::LoseOwnership`, not as a poll method on this trait.
pub trait QueuePort {
    fn enqueue(
        &mut self,
        message_id: QueueMessageId,
        delay: Option<Duration>,
    ) -> Result<QueueMessageId, EnqueueError>;

    fn finish(
        &mut self,
        attempt_id: &AttemptId,
        ownership: &OwnershipToken,
        disposition: FinishDisposition,
    ) -> FinishDisposition;
}
