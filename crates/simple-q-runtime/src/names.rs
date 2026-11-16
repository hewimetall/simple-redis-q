//! Names shared by producers and workers.
//!
//! A queue is one non-empty word of lowercase ASCII letters and digits.

use std::time::Duration;

use anyhow::{Result, bail};

pub const LOCK_PREFIX: &str = "lock:";
pub const MESSAGE_VERSION: u32 = 1;
pub const DEFAULT_QUEUE: &str = "default";
/// First field of a worker name. The queue is the second field.
pub const WORKER_NAME_PREFIX: &str = "agent-worker";
pub const HEARTBEAT_INTERVAL_SECS: u64 = 15;
pub const MISSED_HEARTBEATS: usize = 4;
/// How often an idle worker asks Redis for a task. This is not the heartbeat.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
/// How often a child sends a Python ping. The lifecycle tick is what wakes it.
pub const DEFAULT_PING_INTERVAL_MS: u64 = 2_000;
/// Timeout of one `ping` stage. Scheduling and waiting each get this long.
pub const DEFAULT_PING_TIMEOUT_MS: u64 = 3_000;
/// How often the child wakes for drain and for a ping that is due.
pub const DEFAULT_LIFECYCLE_INTERVAL_MS: u64 = 200;
pub const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 60;
pub const DEFAULT_BATCH_SIZE: u64 = 1;
pub const DEFAULT_IDEMPOTENCY_WAIT_MS: u64 = 20;
pub const DEFAULT_IDEMPOTENCY_WAIT_MAX_MS: u64 = 5_000;
pub const DEFAULT_TIME_LIMIT_MS: u64 = 30 * 60 * 1000;
pub const DEFAULT_MIN_BACKOFF_MS: u64 = 15_000;
pub const DEFAULT_MAX_BACKOFF_MS: u64 = 86_400_000 * 7;
pub const PING_MARGIN_SECS: u64 = 60;
pub const DEFAULT_DONE_TTL_SECS: u64 = 24 * 60 * 60;
pub const DEFAULT_UNIQUE_TTL_SECS: u64 = 24 * 60 * 60;
const _: () = assert!(DEFAULT_UNIQUE_TTL_SECS > 0);
pub const DEFAULT_DEAD_TTL_SECS: u64 = 7 * 24 * 60 * 60;
pub const DEFAULT_MAX_DELIVERIES: u32 = 5;
pub const SWEEP_BATCH: usize = 100;

pub fn default_ping_budget() -> Duration {
    Duration::from_secs(DEFAULT_TIME_LIMIT_MS / 1000 + PING_MARGIN_SECS)
}

pub fn validate_queue(queue: &str) -> Result<()> {
    if queue.is_empty()
        || !queue
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        bail!("queue `{queue}` must be a lowercase ascii word such as `default`");
    }
    Ok(())
}

pub fn lock_redis_key(lock_key: &str) -> Result<String> {
    validate_lock_key(lock_key)?;
    Ok(format!("{LOCK_PREFIX}{lock_key}"))
}

pub(crate) fn validate_lock_key(lock_key: &str) -> Result<()> {
    let canonical = !lock_key.is_empty()
        && !lock_key.starts_with(LOCK_PREFIX)
        && lock_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'.' | b'_' | b'-'));
    if !canonical {
        bail!(
            "lock_key `{lock_key}` must be a non-empty server key such as `demo:gate` without the `{LOCK_PREFIX}` prefix"
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "names_tests.rs"]
mod tests;
