//! Attempt lifecycle: execution, ownership, and finish are separate axes.
//!
//! Handler return ≠ ACK complete. Ownership loss is not erased when the handler
//! returns. Cancel request ≠ execution stopped.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    Claimed,
    Running,
    /// Cancel requested; Python may still be running.
    CancelRequested,
    /// `ConfirmExecutionStopped` already observed. Next is finish, not another wait.
    Stopped,
    /// Handler returned (success or error path). Ownership axis is independent.
    HandlerFinished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipState {
    Held,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishState {
    NotStarted,
    InFlight,
    Confirmed,
    RejectedStale,
    Unconfirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptLifecycle {
    pub execution: ExecutionState,
    pub ownership: OwnershipState,
    pub finish: FinishState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptEvent {
    StartHandler,
    RequestCancel,
    ConfirmExecutionStopped,
    LoseOwnership,
    HandlerReturned,
    BeginFinish,
    FinishConfirmed,
    FinishRejectedStale,
    FinishUnconfirmed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptTransitionError {
    pub lifecycle: AttemptLifecycle,
    pub event: AttemptEvent,
}

impl AttemptLifecycle {
    pub fn claimed() -> Self {
        Self {
            execution: ExecutionState::Claimed,
            ownership: OwnershipState::Held,
            finish: FinishState::NotStarted,
        }
    }

    pub fn ownership_lost(&self) -> bool {
        self.ownership == OwnershipState::Lost
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.finish,
            FinishState::Confirmed | FinishState::RejectedStale | FinishState::Unconfirmed
        )
    }

    /// Disposition hint for the finish adapter: lost ownership must not Ack.
    pub fn allowed_success_ack(&self) -> bool {
        self.ownership == OwnershipState::Held && self.finish == FinishState::InFlight
    }

    pub fn transition(self, event: AttemptEvent) -> Result<Self, AttemptTransitionError> {
        let mut next = self;
        let ok = match event {
            AttemptEvent::StartHandler => {
                if next.execution == ExecutionState::Claimed
                    && next.finish == FinishState::NotStarted
                {
                    next.execution = ExecutionState::Running;
                    true
                } else {
                    false
                }
            }
            AttemptEvent::RequestCancel => match next.execution {
                // Lost ownership uses the same cancel. A second request is a no-op.
                ExecutionState::Running | ExecutionState::CancelRequested
                    if next.finish == FinishState::NotStarted =>
                {
                    next.execution = ExecutionState::CancelRequested;
                    true
                }
                _ => false,
            },
            AttemptEvent::LoseOwnership => {
                if next.is_terminal() {
                    false
                } else {
                    next.ownership = OwnershipState::Lost;
                    true
                }
            }
            AttemptEvent::ConfirmExecutionStopped => match next.execution {
                ExecutionState::CancelRequested | ExecutionState::Running
                    if next.finish == FinishState::NotStarted =>
                {
                    // Running + Confirm is allowed when ownership already lost
                    // (lease loss path without prior RequestCancel).
                    if next.execution == ExecutionState::Running
                        && next.ownership != OwnershipState::Lost
                    {
                        false
                    } else {
                        next.execution = ExecutionState::Stopped;
                        true
                    }
                }
                _ => false,
            },
            AttemptEvent::HandlerReturned => match next.execution {
                ExecutionState::Running | ExecutionState::CancelRequested
                    if next.finish == FinishState::NotStarted =>
                {
                    next.execution = ExecutionState::HandlerFinished;
                    true
                }
                _ => false,
            },
            AttemptEvent::BeginFinish => match next.execution {
                ExecutionState::HandlerFinished | ExecutionState::Stopped
                    if next.finish == FinishState::NotStarted =>
                {
                    next.finish = FinishState::InFlight;
                    true
                }
                _ => false,
            },
            AttemptEvent::FinishConfirmed => {
                if next.execution == ExecutionState::HandlerFinished
                    || next.execution == ExecutionState::Stopped
                {
                    if next.finish == FinishState::InFlight {
                        next.finish = FinishState::Confirmed;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            AttemptEvent::FinishRejectedStale => {
                if (next.execution == ExecutionState::HandlerFinished
                    || next.execution == ExecutionState::Stopped)
                    && next.finish == FinishState::InFlight
                {
                    next.finish = FinishState::RejectedStale;
                    true
                } else {
                    false
                }
            }
            AttemptEvent::FinishUnconfirmed => {
                if (next.execution == ExecutionState::HandlerFinished
                    || next.execution == ExecutionState::Stopped)
                    && next.finish == FinishState::InFlight
                {
                    next.finish = FinishState::Unconfirmed;
                    true
                } else {
                    false
                }
            }
        };

        if ok {
            Ok(next)
        } else {
            Err(AttemptTransitionError {
                lifecycle: self,
                event,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_handler_running_when_lease_lost_then_not_terminal() {
        // Arrange
        let life = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap();

        // Act
        let next = life.transition(AttemptEvent::LoseOwnership).unwrap();

        // Assert
        assert!(next.ownership_lost());
        assert_eq!(next.execution, ExecutionState::Running);
        assert!(!next.is_terminal());
    }

    #[test]
    fn given_ownership_lost_when_handler_returns_then_ownership_lost_is_kept() {
        // Arrange
        let life = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::LoseOwnership)
            .unwrap();

        // Act
        let next = life.transition(AttemptEvent::HandlerReturned).unwrap();

        // Assert
        assert_eq!(next.execution, ExecutionState::HandlerFinished);
        assert!(
            next.ownership_lost(),
            "HandlerReturned must not erase ownership loss"
        );
        assert!(!next.allowed_success_ack());
    }

    #[test]
    fn given_ownership_lost_and_handler_finished_when_finish_in_flight_then_ack_forbidden() {
        // Arrange
        let finishing = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::LoseOwnership)
            .unwrap()
            .transition(AttemptEvent::HandlerReturned)
            .unwrap()
            .transition(AttemptEvent::BeginFinish)
            .unwrap();

        // Act
        let may_ack = finishing.allowed_success_ack();
        let rejected = finishing
            .transition(AttemptEvent::FinishRejectedStale)
            .unwrap();

        // Assert
        assert!(
            !may_ack,
            "adapter must not Ack after ownership loss even if finish is InFlight"
        );
        assert!(rejected.ownership_lost());
        assert!(rejected.is_terminal());
        assert_eq!(rejected.finish, FinishState::RejectedStale);
    }

    #[test]
    fn given_cancel_requested_when_python_not_stopped_then_begin_finish_forbidden() {
        // Arrange
        let life = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::RequestCancel)
            .unwrap();

        // Act / Assert
        assert_eq!(life.execution, ExecutionState::CancelRequested);
        assert!(life.transition(AttemptEvent::BeginFinish).is_err());
        assert!(life.transition(AttemptEvent::FinishConfirmed).is_err());
    }

    #[test]
    fn given_cancel_confirmed_when_stopped_then_state_is_execution_stopped() {
        // Arrange / Act
        let life = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::RequestCancel)
            .unwrap()
            .transition(AttemptEvent::ConfirmExecutionStopped)
            .unwrap();

        // Assert
        assert_eq!(life.execution, ExecutionState::Stopped);
        assert!(!life.ownership_lost());
    }

    #[test]
    fn given_handler_finished_with_ownership_when_finish_unconfirmed_then_terminal() {
        // Arrange
        let finishing = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::HandlerReturned)
            .unwrap()
            .transition(AttemptEvent::BeginFinish)
            .unwrap();

        // Act
        let terminal = finishing
            .transition(AttemptEvent::FinishUnconfirmed)
            .unwrap();

        // Assert
        assert!(terminal.is_terminal());
        assert!(!terminal.ownership_lost());
        assert_eq!(terminal.finish, FinishState::Unconfirmed);
    }

    #[test]
    fn given_running_when_ownership_lost_then_stop_and_stale_finish_keep_lost() {
        // Arrange
        let running = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap();

        // Act
        let lost = running.transition(AttemptEvent::LoseOwnership).unwrap();
        let stopped = lost
            .transition(AttemptEvent::ConfirmExecutionStopped)
            .unwrap();
        let finishing = stopped.transition(AttemptEvent::BeginFinish).unwrap();
        let terminal = finishing
            .transition(AttemptEvent::FinishRejectedStale)
            .unwrap();

        // Assert
        assert!(lost.ownership_lost());
        assert!(!lost.allowed_success_ack());
        assert_eq!(stopped.execution, ExecutionState::Stopped);
        assert!(stopped.ownership_lost());
        assert!(!finishing.allowed_success_ack());
        assert_eq!(terminal.finish, FinishState::RejectedStale);
        assert!(terminal.is_terminal());
        assert!(terminal.transition(AttemptEvent::LoseOwnership).is_err());
    }

    #[test]
    fn given_each_legal_event_when_applied_in_order_then_finish_confirms() {
        // Arrange
        let life = AttemptLifecycle::claimed();

        // Act
        let running = life.transition(AttemptEvent::StartHandler).unwrap();
        let again = running.transition(AttemptEvent::RequestCancel).unwrap();
        let idempotent = again.transition(AttemptEvent::RequestCancel).unwrap();
        let finished = running.transition(AttemptEvent::HandlerReturned).unwrap();
        let inflight = finished.transition(AttemptEvent::BeginFinish).unwrap();
        let confirmed = inflight.transition(AttemptEvent::FinishConfirmed).unwrap();

        // Assert
        assert_eq!(idempotent.execution, ExecutionState::CancelRequested);
        assert!(inflight.allowed_success_ack());
        assert_eq!(confirmed.finish, FinishState::Confirmed);
        assert!(confirmed.is_terminal());
        assert!(confirmed.transition(AttemptEvent::FinishConfirmed).is_err());
        assert!(
            life.transition(AttemptEvent::StartHandler)
                .unwrap()
                .transition(AttemptEvent::StartHandler)
                .is_err()
        );
        assert!(life.transition(AttemptEvent::RequestCancel).is_err());
    }

    #[test]
    fn given_lost_running_when_cancel_requested_then_cancel_is_kept() {
        // Arrange
        let lost = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::LoseOwnership)
            .unwrap();

        // Act
        let cancel = lost.transition(AttemptEvent::RequestCancel).unwrap();
        let returned = cancel.transition(AttemptEvent::HandlerReturned).unwrap();

        // Assert
        assert_eq!(cancel.execution, ExecutionState::CancelRequested);
        assert!(cancel.ownership_lost());
        assert_eq!(returned.execution, ExecutionState::HandlerFinished);
        assert!(returned.ownership_lost());
    }

    #[test]
    fn given_illegal_event_when_applied_then_the_lifecycle_is_unchanged() {
        // Arrange
        let claimed = AttemptLifecycle::claimed();
        let running = claimed.transition(AttemptEvent::StartHandler).unwrap();
        let stopped = running
            .transition(AttemptEvent::LoseOwnership)
            .unwrap()
            .transition(AttemptEvent::ConfirmExecutionStopped)
            .unwrap()
            .transition(AttemptEvent::BeginFinish)
            .unwrap();

        // Act / Assert
        assert!(
            claimed
                .transition(AttemptEvent::ConfirmExecutionStopped)
                .is_err()
        );
        assert!(claimed.transition(AttemptEvent::HandlerReturned).is_err());
        assert!(
            claimed
                .transition(AttemptEvent::FinishRejectedStale)
                .is_err()
        );
        assert!(claimed.transition(AttemptEvent::FinishUnconfirmed).is_err());
        assert!(
            running
                .transition(AttemptEvent::ConfirmExecutionStopped)
                .is_err()
        );
        assert_eq!(
            stopped
                .transition(AttemptEvent::FinishUnconfirmed)
                .unwrap()
                .finish,
            FinishState::Unconfirmed
        );
        assert_eq!(
            stopped
                .transition(AttemptEvent::FinishRejectedStale)
                .unwrap()
                .finish,
            FinishState::RejectedStale
        );
    }
}
