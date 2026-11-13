//! Core contracts for the architectural fork.
//!
//! No Redis, PyO3, or OS-process types leak here. Adapters live outside.

pub mod attempt;
pub mod drain;
pub mod parent;
pub mod ports;

pub use attempt::{AttemptEvent, AttemptState, AttemptTransitionError};
pub use drain::{AdmissionGate, DrainPhase};
pub use parent::{ParentBootstrap, ParentConfigSource, ParentInitError};
pub use ports::{
    AttemptId, BridgeOutcome, FinishDisposition, OwnershipToken, QueueMessageId, WorkerId,
};
