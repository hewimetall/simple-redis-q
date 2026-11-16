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
    /// Held by apalis-redis while this attempt is outstanding.
    ///
    /// Retries carry the same string so the final outcome can drop the key.
    /// A payload written before this field existed has no key and still parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_key: Option<String>,
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
            if self.lock_ttl == 0 {
                bail!("lock_ttl must be greater than zero");
            }
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
#[path = "job_tests.rs"]
mod tests;
