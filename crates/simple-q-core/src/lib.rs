//! Core contracts for the architectural fork.
//!
//! No Redis, PyO3, or OS-process types leak here. Adapters live outside.

pub mod attempt;
pub mod drain;
pub mod parent;
pub mod ports;

pub use attempt::{
    AttemptEvent, AttemptLifecycle, AttemptTransitionError, ExecutionState, FinishState,
    OwnershipState,
};
pub use drain::{AdmissionGate, ClaimAdmission, DrainPhase};
pub use parent::{ParentBootstrap, ParentConfigSource, ParentInitError};
pub use ports::{
    AttemptId, BridgeOutcome, FinishDisposition, OwnershipToken, QueueMessageId, WorkerId,
};
