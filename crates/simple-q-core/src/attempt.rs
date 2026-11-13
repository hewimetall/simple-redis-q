//! Attempt state machine: handler return ≠ ACK complete; lease loss ≠ terminal.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    Claimed,
    HandlerRunning,
    /// Cancel requested; Python may still be running.
    CancelRequested,
    /// Waiting for Python stop after cancel or lease loss.
    StoppingExecution,
    /// Ownership lost; local handler may still run until stop confirms.
    OwnershipLostWhileRunning,
    HandlerFinished,
    Finishing,
    Terminal,
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
    pub from: AttemptState,
    pub event: AttemptEvent,
}

impl AttemptState {
    pub fn transition(self, event: AttemptEvent) -> Result<Self, AttemptTransitionError> {
        use AttemptEvent::*;
        use AttemptState::*;
        let next = match (self, event) {
            (Claimed, StartHandler) => HandlerRunning,
            (HandlerRunning, RequestCancel) => CancelRequested,
            (HandlerRunning, LoseOwnership) => OwnershipLostWhileRunning,
            (HandlerRunning, HandlerReturned) => HandlerFinished,
            (CancelRequested, ConfirmExecutionStopped) => StoppingExecution,
            (CancelRequested, LoseOwnership) => OwnershipLostWhileRunning,
            (CancelRequested, HandlerReturned) => HandlerFinished,
            (OwnershipLostWhileRunning, ConfirmExecutionStopped) => StoppingExecution,
            (OwnershipLostWhileRunning, HandlerReturned) => HandlerFinished,
            (OwnershipLostWhileRunning, RequestCancel) => OwnershipLostWhileRunning,
            (StoppingExecution, BeginFinish) => Finishing,
            (HandlerFinished, BeginFinish) => Finishing,
            (Finishing, FinishConfirmed | FinishRejectedStale | FinishUnconfirmed) => Terminal,
            _ => {
                return Err(AttemptTransitionError {
                    from: self,
                    event,
                });
            }
        };
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_handler_running_when_lease_lost_then_not_terminal() {
        // Arrange
        let state = AttemptState::HandlerRunning;

        // Act
        let next = state
            .transition(AttemptEvent::LoseOwnership)
            .expect("lease loss is allowed");

        // Assert
        assert_eq!(next, AttemptState::OwnershipLostWhileRunning);
        assert_ne!(next, AttemptState::Terminal);
    }

    #[test]
    fn given_ownership_lost_when_handler_still_returns_then_finishing_required() {
        // Arrange
        let state = AttemptState::OwnershipLostWhileRunning
            .transition(AttemptEvent::HandlerReturned)
            .unwrap();

        // Act
        let direct_terminal = state.transition(AttemptEvent::FinishConfirmed);
        let via_finish = state
            .transition(AttemptEvent::BeginFinish)
            .unwrap()
            .transition(AttemptEvent::FinishRejectedStale)
            .unwrap();

        // Assert
        assert!(direct_terminal.is_err(), "must enter Finishing before Terminal");
        assert_eq!(via_finish, AttemptState::Terminal);
    }

    #[test]
    fn given_cancel_requested_when_python_not_stopped_then_not_terminal() {
        // Arrange
        let state = AttemptState::HandlerRunning
            .transition(AttemptEvent::RequestCancel)
            .unwrap();

        // Act / Assert
        assert_eq!(state, AttemptState::CancelRequested);
        assert!(state.transition(AttemptEvent::FinishConfirmed).is_err());
        assert!(state.transition(AttemptEvent::BeginFinish).is_err());
    }

    #[test]
    fn given_handler_finished_when_finish_unconfirmed_then_terminal_with_disposition_elsewhere() {
        // Arrange
        let finishing = AttemptState::HandlerFinished
            .transition(AttemptEvent::BeginFinish)
            .unwrap();

        // Act
        let terminal = finishing
            .transition(AttemptEvent::FinishUnconfirmed)
            .unwrap();

        // Assert
        assert_eq!(terminal, AttemptState::Terminal);
    }
}
