//! Attempt lifecycle and the drain admission gate.
//!
//! No Redis client, PyO3 type, or OS process type appears here.

pub mod attempt;
pub mod drain;

pub use attempt::{
    AttemptEvent, AttemptLifecycle, AttemptTransitionError, ExecutionState, FinishState,
    OwnershipState,
};
pub use drain::{AdmissionGate, ClaimAdmission, DrainPhase};
