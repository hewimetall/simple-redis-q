//! Closed bridge outcomes for the child.
//!
//! PyO3 stays in the child process. This crate maps the handler dict and
//! chooses a finish disposition. An unknown `outcome` string is a protocol
//! error. A missing `outcome` stays success, matching the worker's previous
//! treatment of `ok` and of a dict that omits the key.

use std::time::Duration;

use serde_json::Value;
use simple_q_backend::FinishDisposition;
use simple_q_orchestration::{AttemptLifecycle, OwnershipState};

/// Closed bridge outcomes. Unknown wire values are protocol errors at parse time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeOutcome {
    Ok,
    Retry { delay: Duration },
    Dead { reason: String },
    Skipped,
    ApplicationError { message: String },
    Timeout,
    CancelConfirmed,
    LeaseLost,
    BridgeFailure { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    pub outcome: String,
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "unknown bridge outcome `{}`", self.outcome)
    }
}

impl std::error::Error for ProtocolError {}

/// Parse the dict `App.dispatch` returned.
pub fn parse_outcome(value: &Value) -> Result<BridgeOutcome, ProtocolError> {
    let Some(outcome) = value.get("outcome").and_then(Value::as_str) else {
        return Ok(BridgeOutcome::Ok);
    };
    match outcome {
        "ok" => Ok(BridgeOutcome::Ok),
        "skipped" => Ok(BridgeOutcome::Skipped),
        "retry" => {
            let delay_ms = value.get("delay_ms").and_then(Value::as_u64).unwrap_or(0);
            Ok(BridgeOutcome::Retry {
                delay: Duration::from_millis(delay_ms),
            })
        }
        "dead" => {
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .unwrap_or("dead")
                .to_owned();
            Ok(BridgeOutcome::Dead { reason })
        }
        "lease_lost" => Ok(BridgeOutcome::LeaseLost),
        "timeout" => Ok(BridgeOutcome::Timeout),
        "cancel" => Ok(BridgeOutcome::CancelConfirmed),
        "error" => {
            let message = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("application error")
                .to_owned();
            Ok(BridgeOutcome::ApplicationError { message })
        }
        "bridge_failure" => {
            let message = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("bridge failure")
                .to_owned();
            Ok(BridgeOutcome::BridgeFailure { message })
        }
        other => Err(ProtocolError {
            outcome: other.to_owned(),
        }),
    }
}

/// Disposition after the handler returns.
///
/// Lost ownership, including `LeaseLost`, is never `Ack`.
pub fn finish_for(life: AttemptLifecycle, outcome: &BridgeOutcome) -> FinishDisposition {
    if life.ownership == OwnershipState::Lost || matches!(outcome, BridgeOutcome::LeaseLost) {
        return FinishDisposition::RejectedStaleOwnership;
    }
    if !life.allowed_success_ack() {
        return match outcome {
            BridgeOutcome::BridgeFailure { message } => FinishDisposition::FinishUnconfirmed {
                detail: message.clone(),
            },
            _ => FinishDisposition::RejectedStaleOwnership,
        };
    }
    match outcome {
        BridgeOutcome::Ok | BridgeOutcome::Skipped => FinishDisposition::Ack,
        BridgeOutcome::Retry { delay } => FinishDisposition::Retry { delay: *delay },
        BridgeOutcome::Dead { reason } => FinishDisposition::Dead {
            reason: reason.clone(),
        },
        BridgeOutcome::ApplicationError { message } => FinishDisposition::Dead {
            reason: message.clone(),
        },
        BridgeOutcome::Timeout => FinishDisposition::Dead {
            reason: "timeout".to_owned(),
        },
        BridgeOutcome::CancelConfirmed => FinishDisposition::Dead {
            reason: "cancel".to_owned(),
        },
        BridgeOutcome::BridgeFailure { message } => FinishDisposition::FinishUnconfirmed {
            detail: message.clone(),
        },
        BridgeOutcome::LeaseLost => FinishDisposition::RejectedStaleOwnership,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use simple_q_orchestration::AttemptEvent;

    fn finishing(lost: bool) -> AttemptLifecycle {
        let mut life = AttemptLifecycle::claimed()
            .transition(AttemptEvent::StartHandler)
            .unwrap()
            .transition(AttemptEvent::HandlerReturned)
            .unwrap();
        if lost {
            life = life.transition(AttemptEvent::LoseOwnership).unwrap();
        }
        life.transition(AttemptEvent::BeginFinish).unwrap()
    }

    #[test]
    fn given_ok_skipped_and_missing_when_parsed_then_success_outcomes() {
        // Arrange
        let ok = json!({"outcome": "ok"});
        let skipped = json!({"outcome": "skipped"});
        let missing = json!({"result": 1});

        // Act
        let ok = parse_outcome(&ok).unwrap();
        let skipped = parse_outcome(&skipped).unwrap();
        let missing = parse_outcome(&missing).unwrap();

        // Assert
        assert_eq!(ok, BridgeOutcome::Ok);
        assert_eq!(skipped, BridgeOutcome::Skipped);
        assert_eq!(missing, BridgeOutcome::Ok);
    }

    #[test]
    fn given_retry_when_parsed_then_delay_is_read() {
        // Arrange
        let value = json!({"outcome": "retry", "delay_ms": 25});

        // Act
        let outcome = parse_outcome(&value).unwrap();

        // Assert
        assert_eq!(
            outcome,
            BridgeOutcome::Retry {
                delay: Duration::from_millis(25)
            }
        );
    }

    #[test]
    fn given_unknown_outcome_when_parsed_then_protocol_error() {
        // Arrange
        let value = json!({"outcome": "maybe"});

        // Act
        let error = parse_outcome(&value).unwrap_err();

        // Assert
        assert_eq!(error.outcome, "maybe");
    }

    #[test]
    fn given_ownership_held_when_handler_ok_then_ack() {
        // Arrange
        let life = finishing(false);

        // Act
        let disposition = finish_for(life, &BridgeOutcome::Ok);

        // Assert
        assert_eq!(disposition, FinishDisposition::Ack);
    }

    #[test]
    fn given_ownership_lost_when_handler_ok_then_stale_not_ack() {
        // Arrange
        let life = finishing(true);

        // Act
        let disposition = finish_for(life, &BridgeOutcome::Ok);

        // Assert
        assert_eq!(disposition, FinishDisposition::RejectedStaleOwnership);
        assert!(!life.allowed_success_ack());
    }

    #[test]
    fn given_lease_lost_outcome_when_finish_then_stale() {
        // Arrange
        let life = finishing(false);

        // Act
        let disposition = finish_for(life, &BridgeOutcome::LeaseLost);

        // Assert
        assert_eq!(disposition, FinishDisposition::RejectedStaleOwnership);
    }
}
