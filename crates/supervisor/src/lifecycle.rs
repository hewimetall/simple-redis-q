//! Parent slot state machine. One owner applies every transition.

use std::sync::Mutex;

/// `Starting → Running → Draining → Stopping → Exited`, or `Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessPhase {
    Starting,
    Running,
    Draining,
    Stopping,
    Exited,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessEvent {
    Ready,
    StartFailed,
    Drain,
    Stop,
    Reaped,
    Crash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessTransitionError {
    pub from: ProcessPhase,
    pub event: ProcessEvent,
}

/// One slot. Copying the value does not share the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotLifecycle {
    phase: ProcessPhase,
}

impl SlotLifecycle {
    pub fn starting() -> Self {
        Self {
            phase: ProcessPhase::Starting,
        }
    }

    pub fn phase(self) -> ProcessPhase {
        self.phase
    }

    /// Second drain while `Draining` is a no-op.
    ///
    /// `Running → Exited` is forbidden: `Reaped` is accepted only from
    /// `Stopping` or `Failed`.
    pub fn transition(self, event: ProcessEvent) -> Result<Self, ProcessTransitionError> {
        let phase = match (self.phase, event) {
            (ProcessPhase::Starting, ProcessEvent::Ready) => ProcessPhase::Running,
            (ProcessPhase::Starting, ProcessEvent::StartFailed) => ProcessPhase::Failed,
            (ProcessPhase::Running, ProcessEvent::Drain) => ProcessPhase::Draining,
            (ProcessPhase::Running, ProcessEvent::Stop) => ProcessPhase::Stopping,
            (ProcessPhase::Running, ProcessEvent::Crash) => ProcessPhase::Failed,
            (ProcessPhase::Draining, ProcessEvent::Drain) => ProcessPhase::Draining,
            (ProcessPhase::Draining, ProcessEvent::Stop) => ProcessPhase::Stopping,
            (ProcessPhase::Draining, ProcessEvent::Crash) => ProcessPhase::Failed,
            (ProcessPhase::Stopping, ProcessEvent::Stop) => ProcessPhase::Stopping,
            (ProcessPhase::Stopping, ProcessEvent::Reaped) => ProcessPhase::Exited,
            (ProcessPhase::Failed, ProcessEvent::Reaped) => ProcessPhase::Exited,
            (from, event) => {
                return Err(ProcessTransitionError { from, event });
            }
        };
        Ok(Self { phase })
    }
}

/// Single transition owner for one slot.
#[derive(Debug)]
pub struct SlotOwner {
    inner: Mutex<SlotLifecycle>,
}

impl SlotOwner {
    pub fn starting() -> Self {
        Self {
            inner: Mutex::new(SlotLifecycle::starting()),
        }
    }

    pub fn apply(&self, event: ProcessEvent) -> Result<ProcessPhase, ProcessTransitionError> {
        let mut life = self.inner.lock().expect("slot owner");
        *life = life.transition(event)?;
        Ok(life.phase())
    }

    pub fn phase(&self) -> ProcessPhase {
        self.inner.lock().expect("slot owner").phase()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn given_running_when_reaped_then_rejected() {
        // Arrange
        let running = SlotLifecycle::starting()
            .transition(ProcessEvent::Ready)
            .unwrap();

        // Act
        let error = running.transition(ProcessEvent::Reaped).unwrap_err();

        // Assert
        assert_eq!(error.from, ProcessPhase::Running);
        assert_eq!(error.event, ProcessEvent::Reaped);
        assert_eq!(running.phase(), ProcessPhase::Running);
    }

    #[test]
    fn given_draining_when_drained_again_then_still_draining() {
        // Arrange
        let draining = SlotLifecycle::starting()
            .transition(ProcessEvent::Ready)
            .unwrap()
            .transition(ProcessEvent::Drain)
            .unwrap();

        // Act
        let again = draining.transition(ProcessEvent::Drain).unwrap();

        // Assert
        assert_eq!(again.phase(), ProcessPhase::Draining);
    }

    #[test]
    fn given_stopping_when_reaped_then_exited() {
        // Arrange
        let stopping = SlotLifecycle::starting()
            .transition(ProcessEvent::Ready)
            .unwrap()
            .transition(ProcessEvent::Stop)
            .unwrap();

        // Act
        let exited = stopping.transition(ProcessEvent::Reaped).unwrap();

        // Assert
        assert_eq!(exited.phase(), ProcessPhase::Exited);
        assert!(exited.transition(ProcessEvent::Drain).is_err());
    }

    #[test]
    fn given_two_threads_when_stop_and_drain_then_one_owner_stays_consistent() {
        // Arrange
        let owner = Arc::new(SlotOwner::starting());
        owner.apply(ProcessEvent::Ready).unwrap();
        let left = Arc::clone(&owner);
        let right = Arc::clone(&owner);

        // Act
        let drain = thread::spawn(move || left.apply(ProcessEvent::Drain));
        let stop = thread::spawn(move || right.apply(ProcessEvent::Stop));
        let drain = drain.join().expect("drain thread");
        let stop = stop.join().expect("stop thread");

        // Assert
        assert!(drain.is_ok() || stop.is_ok());
        let phase = owner.phase();
        assert!(matches!(
            phase,
            ProcessPhase::Draining | ProcessPhase::Stopping
        ));
    }

    fn settle(owner: &SlotOwner) {
        if owner.phase() == ProcessPhase::Draining {
            assert_eq!(
                owner.apply(ProcessEvent::Stop).unwrap(),
                ProcessPhase::Stopping
            );
        }
        assert_eq!(
            owner.apply(ProcessEvent::Reaped).unwrap(),
            ProcessPhase::Exited
        );
    }

    #[test]
    fn given_either_drain_winner_when_settled_then_the_slot_is_exited() {
        // Arrange
        let drained = SlotOwner::starting();
        drained.apply(ProcessEvent::Ready).unwrap();
        drained.apply(ProcessEvent::Drain).unwrap();
        let stopped = SlotOwner::starting();
        stopped.apply(ProcessEvent::Ready).unwrap();
        stopped.apply(ProcessEvent::Stop).unwrap();

        // Act / Assert
        settle(&drained);
        settle(&stopped);
    }

    fn apply(events: &[ProcessEvent]) -> Result<ProcessPhase, ProcessTransitionError> {
        let mut life = SlotLifecycle::starting();
        for event in events {
            life = life.transition(*event)?;
        }
        Ok(life.phase())
    }

    #[test]
    fn given_every_legal_transition_when_applied_then_phase_matches() {
        // Arrange / Act / Assert
        assert_eq!(
            apply(&[ProcessEvent::StartFailed]).unwrap(),
            ProcessPhase::Failed
        );
        assert_eq!(
            apply(&[ProcessEvent::Ready, ProcessEvent::Crash]).unwrap(),
            ProcessPhase::Failed
        );
        assert_eq!(
            apply(&[ProcessEvent::Ready, ProcessEvent::Drain, ProcessEvent::Stop]).unwrap(),
            ProcessPhase::Stopping
        );
        assert_eq!(
            apply(&[
                ProcessEvent::Ready,
                ProcessEvent::Drain,
                ProcessEvent::Crash
            ])
            .unwrap(),
            ProcessPhase::Failed
        );
        assert_eq!(
            apply(&[ProcessEvent::Ready, ProcessEvent::Stop, ProcessEvent::Stop]).unwrap(),
            ProcessPhase::Stopping
        );
        assert_eq!(
            apply(&[ProcessEvent::StartFailed, ProcessEvent::Reaped]).unwrap(),
            ProcessPhase::Exited
        );
        assert!(apply(&[ProcessEvent::Reaped]).is_err());
        assert!(apply(&[ProcessEvent::Ready, ProcessEvent::Reaped]).is_err());
        assert!(
            SlotLifecycle::starting()
                .transition(ProcessEvent::Crash)
                .is_err()
        );
    }
}
