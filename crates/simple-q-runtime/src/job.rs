//! The message. No Redis and no Python. Producers and workers share this shape.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::names::{self, MESSAGE_VERSION, validate_queue};

/// Wire message stored in Redis.
///
/// `message_id` stays the same across retries. `version` lets a producer and a
/// worker that were deployed at different times reject a payload they cannot read.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub version: u32,
    pub message_id: String,
    pub actor_name: String,
    pub queue: String,
    pub kwargs: Value,
    pub headers: BTreeMap<String, String>,
    pub retries: u32,
    pub max_retries: u32,
    pub time_limit_ms: u64,
    pub min_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_deliveries: u32,
    #[serde(default)]
    pub lock_key: Option<String>,
    #[serde(default = "default_lock_ttl")]
    pub lock_ttl: u64,
    #[serde(default)]
    pub skip_on_lock: bool,
}

fn default_lock_ttl() -> u64 {
    30
}

impl Job {
    pub fn parse(value: Value) -> Result<Self> {
        if value.get("version").is_none() {
            bail!("message is missing version");
        }
        let job: Self =
            serde_json::from_value(value).context("message does not match version 1")?;
        job.validate()?;
        Ok(job)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != MESSAGE_VERSION {
            bail!(
                "unsupported message version {}; this runtime reads version {MESSAGE_VERSION}",
                self.version
            );
        }
        if self.message_id.is_empty() || self.actor_name.is_empty() {
            bail!("message_id and actor_name are required");
        }
        validate_queue(&self.queue)?;
        if !self.kwargs.is_object() {
            bail!("kwargs must be a JSON object");
        }
        if self.time_limit_ms == 0 || self.min_backoff_ms == 0 || self.max_backoff_ms == 0 {
            bail!("time_limit_ms, min_backoff_ms, and max_backoff_ms must be greater than zero");
        }
        if self.max_deliveries == 0 {
            bail!("max_deliveries must be greater than zero");
        }
        if let Some(lock_key) = &self.lock_key {
            names::validate_lock_key(lock_key)?;
        }
        Ok(())
    }
}

pub fn new_message_id() -> String {
    Uuid::new_v4().to_string()
}

/// Worker decision for one counted delivery.
///
/// Retry and timeout stay in Python: they need the exception. Poison is
/// decided here, before the handler runs. A delivery that only hangs the
/// interpreter must still be buried by the next process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Run,
    Poison,
}

/// `deliveries` is the value Redis returned from `INCR`, so the first run is 1.
///
/// A count above `max_deliveries` is poison. The handler must not be entered.
pub fn decide(deliveries: u64, max_deliveries: u32) -> Decision {
    if deliveries > u64::from(max_deliveries) {
        Decision::Poison
    } else {
        Decision::Run
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Value {
        json!({
            "version": 1,
            "message_id": "m-1",
            "actor_name": "discover",
            "queue": "default",
            "kwargs": {"value": 1},
            "headers": {"trace": "abc"},
            "retries": 0,
            "max_retries": 3,
            "time_limit_ms": 1_800_000,
            "min_backoff_ms": 15000,
            "max_backoff_ms": 60000,
            "max_deliveries": 5
        })
    }

    #[test]
    fn version_one_round_trips_and_keeps_headers_out_of_kwargs() {
        let job = Job::parse(sample()).unwrap();
        assert_eq!(job.version, 1);
        assert_eq!(job.headers.get("trace").map(String::as_str), Some("abc"));
        assert!(job.kwargs.get("headers").is_none());
        assert!(job.kwargs.get("trace").is_none());
    }

    #[test]
    fn a_missing_or_foreign_version_is_rejected() {
        let mut missing = sample();
        missing.as_object_mut().unwrap().remove("version");
        assert!(
            Job::parse(missing)
                .unwrap_err()
                .to_string()
                .contains("version")
        );

        let mut foreign = sample();
        foreign["version"] = json!(2);
        assert!(
            Job::parse(foreign)
                .unwrap_err()
                .to_string()
                .contains("unsupported message version")
        );
    }

    #[test]
    fn a_delivery_past_the_limit_is_poison_and_the_limit_itself_still_runs() {
        assert_eq!(decide(1, 1), Decision::Run);
        assert_eq!(decide(2, 1), Decision::Poison);
        assert_eq!(decide(5, 5), Decision::Run);
        assert_eq!(decide(6, 5), Decision::Poison);
    }

    #[test]
    fn the_message_module_does_not_open_redis() {
        let source = include_str!("job.rs");
        for line in source.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("use ") || trimmed.starts_with("extern ") {
                for crate_name in ["redis", "pyo3", "tokio", "apalis"] {
                    assert!(
                        !trimmed.contains(crate_name),
                        "job.rs imports {crate_name}: {trimmed}"
                    );
                }
            }
        }
    }
}
