//! Public names shared by the CLI, Redis keys, and the Python facade.
//!
//! Product queues are single lowercase words. `fanout` is the runtime queue.
//! `fan-out` is the existing Python mechanism and is not a queue name.

use anyhow::{Result, bail};

pub const LOCK_PREFIX: &str = "lock:";
pub const HEARTBEAT_INTERVAL_SECS: u64 = 15;
pub const MISSED_HEARTBEATS: usize = 4;

pub fn orphan_window_secs() -> u64 {
    HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS as u64
}

pub fn validate_queue(queue: &str) -> Result<()> {
    if queue.is_empty()
        || !queue
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        bail!("queue `{queue}` must be a lowercase ascii word such as `control` or `fanout`");
    }
    Ok(())
}

pub fn lock_redis_key(lock_key: &str) -> Result<String> {
    validate_lock_key(lock_key)?;
    Ok(format!("{LOCK_PREFIX}{lock_key}"))
}

pub fn validate_lock_key(lock_key: &str) -> Result<()> {
    if lock_key.is_empty()
        || lock_key.starts_with(LOCK_PREFIX)
        || lock_key.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        bail!(
            "lock_key `{lock_key}` must be a non-empty server key such as `cache:discovery` without the `{LOCK_PREFIX}` prefix"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_queue_names_are_single_lowercase_words() {
        assert!(validate_queue("control").is_ok());
        assert!(validate_queue("fanout").is_ok());
        assert!(validate_queue("fan-out").is_err());
        assert!(validate_queue("fan_out").is_err());
        assert!(validate_queue("simple-q").is_err());
        assert!(validate_queue("").is_err());
    }

    #[test]
    fn lock_keys_keep_a_prefix_outside_the_apalis_keyspace() {
        assert_eq!(
            lock_redis_key("cache:discovery").unwrap(),
            "lock:cache:discovery"
        );
        assert!(lock_redis_key("lock:cache:discovery").is_err());
        assert!(lock_redis_key("").is_err());
        assert!(lock_redis_key("cache discovery").is_err());
    }

    #[test]
    fn orphan_window_is_explicit() {
        assert_eq!(orphan_window_secs(), 60);
    }
}
