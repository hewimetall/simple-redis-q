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
pub const DEFAULT_TIME_LIMIT_MS: u64 = 30 * 60 * 1000;
pub const DEFAULT_MIN_BACKOFF_MS: u64 = 15_000;
pub const DEFAULT_MAX_BACKOFF_MS: u64 = 86_400_000 * 7;
pub const PING_MARGIN_SECS: u64 = 60;
pub const DEFAULT_DONE_TTL_SECS: u64 = 24 * 60 * 60;
pub const DEFAULT_UNIQUE_TTL_SECS: u64 = 24 * 60 * 60;
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
mod tests {
    use super::*;

    #[test]
    fn a_queue_name_is_one_lowercase_word() {
        assert!(validate_queue(DEFAULT_QUEUE).is_ok());
        assert!(validate_queue("holdq").is_ok());
        assert!(validate_queue("fan-out").is_err());
        assert!(validate_queue("fan_out").is_err());
        assert!(validate_queue("simple-q").is_err());
        assert!(validate_queue("").is_err());
        let error = validate_queue("Bad").unwrap_err().to_string();
        assert!(error.contains("default"), "{error}");
    }

    #[test]
    fn lock_keys_keep_a_prefix_outside_the_apalis_keyspace() {
        assert_eq!(lock_redis_key("demo:gate").unwrap(), "lock:demo:gate");
        assert!(lock_redis_key("lock:demo:gate").is_err());
        assert!(lock_redis_key("").is_err());
        assert!(lock_redis_key("demo gate").is_err());
        assert!(lock_redis_key("demo:bad\u{200b}").is_err());
    }

    #[test]
    fn orphan_window_is_inside_the_redelivery_bound() {
        assert_eq!(HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS as u64, 60);
        assert!(HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS as u64 <= 75);
    }

    #[test]
    fn ping_budget_covers_the_default_time_limit_and_a_minute_block() {
        assert_eq!(DEFAULT_TIME_LIMIT_MS, 1_800_000);
        assert!(default_ping_budget() >= Duration::from_secs(30 * 60 + 60));
        assert!(default_ping_budget() > Duration::from_secs(60));
    }

    #[test]
    fn dead_messages_are_kept_for_seven_days() {
        assert_eq!(DEFAULT_DEAD_TTL_SECS, 7 * 24 * 60 * 60);
    }

    #[test]
    fn unique_keys_keep_a_positive_ttl() {
        // Zero is not a TTL. The idempotency script treats the string `"0"` as
        // present and `EXPIRE 0` deletes the key immediately.
        assert_eq!(DEFAULT_UNIQUE_TTL_SECS, 86_400);
        assert!(DEFAULT_UNIQUE_TTL_SECS > 0);
    }
}
