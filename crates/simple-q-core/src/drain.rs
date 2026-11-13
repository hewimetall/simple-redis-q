//! Drain barrier: admission gate ≠ TaskTracker.
//!
//! Tokio `TaskTracker::close` only stops *tracking new tasks on that tracker*.
//! Closing Redis/backend claim admission is a separate step that must happen
//! first (or atomically with "no new Attempt accepted").

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainPhase {
    Accepting,
    /// New claims rejected; in-flight claims may still become Attempts.
    AdmissionClosed,
    /// All accepted Attempts registered; waiting tracked finish work.
    WaitingTrackedWork,
    Hooks,
    ChildExit,
    Reaped,
}

#[derive(Debug, Default)]
pub struct AdmissionGate {
    closed: bool,
    /// Claims that won the race after close was requested but before reject.
    accepted_after_close: u64,
    rejected: u64,
}

impl AdmissionGate {
    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Decide whether a claim that already completed in Redis may become work.
    ///
    /// Contract: if admission is closed, a late claim is either
    /// `accepted_inflight` (counted, must be tracked) or `rejected` (must be
    /// returned/released by the adapter without starting the handler).
    /// Stage C Apalis adapter must pick one policy and test it; default here
    /// is accept-and-track so drain cannot miss work.
    pub fn note_claim_resolved(&mut self) -> ClaimAdmission {
        if !self.closed {
            ClaimAdmission::Accept
        } else {
            self.accepted_after_close = self.accepted_after_close.saturating_add(1);
            ClaimAdmission::AcceptInflightAfterClose
        }
    }

    pub fn reject_late_claim(&mut self) -> ClaimAdmission {
        if !self.closed {
            ClaimAdmission::Accept
        } else {
            self.rejected = self.rejected.saturating_add(1);
            ClaimAdmission::Reject
        }
    }

    pub fn accepted_after_close(&self) -> u64 {
        self.accepted_after_close
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimAdmission {
    Accept,
    AcceptInflightAfterClose,
    Reject,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_open_gate_when_claim_then_accept() {
        // Arrange
        let mut gate = AdmissionGate::default();

        // Act
        let decision = gate.note_claim_resolved();

        // Assert
        assert_eq!(decision, ClaimAdmission::Accept);
        assert_eq!(gate.accepted_after_close(), 0);
    }

    #[test]
    fn given_closed_gate_when_inflight_claim_then_still_counted_not_silently_dropped() {
        // Arrange
        let mut gate = AdmissionGate::default();
        gate.close();

        // Act
        let decision = gate.note_claim_resolved();

        // Assert
        assert_eq!(decision, ClaimAdmission::AcceptInflightAfterClose);
        assert_eq!(gate.accepted_after_close(), 1);
        assert_ne!(
            decision,
            ClaimAdmission::Reject,
            "default policy tracks the race; reject is explicit via reject_late_claim"
        );
    }

    #[test]
    fn given_drain_phases_when_ordered_then_waiting_follows_admission_closed() {
        // Arrange / Act
        let phases = [
            DrainPhase::Accepting,
            DrainPhase::AdmissionClosed,
            DrainPhase::WaitingTrackedWork,
            DrainPhase::Hooks,
            DrainPhase::ChildExit,
            DrainPhase::Reaped,
        ];

        // Assert
        assert_eq!(phases[1], DrainPhase::AdmissionClosed);
        assert_eq!(phases[2], DrainPhase::WaitingTrackedWork);
    }
}
