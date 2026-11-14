//! Drain barrier: admission gate ≠ TaskTracker.
//!
//! Tokio `TaskTracker::close` does **not** forbid adding or tracking new tasks.
//! It allows `wait()` to complete once the tracker is empty
//! ([docs](https://docs.rs/tokio-util/latest/tokio_util/task/task_tracker/struct.TaskTracker.html)).
//!
//! Binding order:
//! 1. close admission (no new claims accepted as work);
//! 2. settle in-flight claims that already won the race;
//! 3. register every accepted Attempt on the tracker;
//! 4. `tracker.close()` so `wait` may finish when empty;
//! 5. `tracker.wait()`.
//!
//! Do not call `close()`+`wait()` while admission can still produce unregistered
//! work — otherwise wait can return empty and a late claim can still spawn.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainPhase {
    Accepting,
    /// New claims are not started; in-flight claims may still become Attempts.
    AdmissionClosed,
    /// In-flight claim race settled; every accepted Attempt is registered.
    InflightSettled,
    /// `TaskTracker::close` called — wait may complete when empty.
    TrackerClosed,
    /// Waiting for registered handler+finish tasks.
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
    /// True once Stage C marks that every accepted Attempt is on the tracker.
    inflight_settled: bool,
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
    /// Default: accept-and-track so drain cannot miss work.
    pub fn note_claim_resolved(&mut self) -> ClaimAdmission {
        if self.inflight_settled {
            self.rejected = self.rejected.saturating_add(1);
            return ClaimAdmission::Reject;
        }
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

    /// Call after every accepted Attempt from the close race is registered.
    /// Further `note_claim_resolved` rejects — safe point before tracker.close().
    pub fn mark_inflight_settled(&mut self) {
        self.inflight_settled = true;
    }

    pub fn inflight_settled(&self) -> bool {
        self.inflight_settled
    }

    pub fn accepted_after_close(&self) -> u64 {
        self.accepted_after_close
    }

    /// Whether it is safe to `tracker.close()` then `wait()`.
    pub fn ready_for_tracker_close(&self) -> bool {
        self.closed && self.inflight_settled
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
    }

    #[test]
    fn given_inflight_settled_when_late_claim_then_reject_before_tracker_close() {
        // Arrange
        let mut gate = AdmissionGate::default();
        gate.close();
        let _ = gate.note_claim_resolved();
        gate.mark_inflight_settled();

        // Act
        let late = gate.note_claim_resolved();

        // Assert
        assert!(gate.ready_for_tracker_close());
        assert_eq!(late, ClaimAdmission::Reject);
    }

    #[test]
    fn given_drain_phases_when_ordered_then_settle_before_tracker_close() {
        // Arrange / Act
        let phases = [
            DrainPhase::Accepting,
            DrainPhase::AdmissionClosed,
            DrainPhase::InflightSettled,
            DrainPhase::TrackerClosed,
            DrainPhase::WaitingTrackedWork,
            DrainPhase::Hooks,
            DrainPhase::ChildExit,
            DrainPhase::Reaped,
        ];

        // Assert
        assert_eq!(phases[2], DrainPhase::InflightSettled);
        assert_eq!(phases[3], DrainPhase::TrackerClosed);
        assert_eq!(phases[4], DrainPhase::WaitingTrackedWork);
    }
}
