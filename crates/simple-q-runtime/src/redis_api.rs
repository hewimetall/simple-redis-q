//! Server side. The only module that opens a Redis connection.
//!
//! `install` runs when a producer calls `configure` or a worker starts.
//! Importing the Python module does not call it.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use apalis::prelude::{TaskBuilder, TaskSink};
use apalis_redis::{Config, RedisStorage, connect};
use redis::aio::ConnectionManager;

use crate::job::Job;
use crate::names::{self, orphan_window_secs, validate_queue};

static CONNECTION: OnceLock<ConnectionManager> = OnceLock::new();

pub fn is_configured() -> bool {
    CONNECTION.get().is_some()
}

pub async fn install(url: String) -> Result<()> {
    if CONNECTION.get().is_some() {
        return Ok(());
    }
    let connection = connect(url).await.context("failed to connect to Redis")?;
    CONNECTION
        .set(connection)
        .map_err(|_| anyhow::anyhow!("Redis connection was already installed"))?;
    Ok(())
}

pub async fn install_from_env() -> Result<()> {
    let url = std::env::var("REDIS_URL").context("REDIS_URL is required")?;
    install(url).await
}

pub fn connection() -> Result<ConnectionManager> {
    CONNECTION.get().cloned().context(
        "Redis connection is not installed; call simple_q.configure(redis_url=...) before send",
    )
}

pub fn queue_config(queue: &str) -> Result<Config> {
    validate_queue(queue)?;
    // apalis 1.0.0 uses this flag as the ack switch. Turning it off leaves the
    // message inflight. Finished payloads are removed by the sweeper instead.
    let config = Config::default()
        .queue(queue)
        .batch_size(1)
        .lock_tasks(true)
        .heartbeat_interval(Duration::from_secs(names::heartbeat_interval_secs()))
        .missed_heartbeats(names::missed_heartbeats())
        .emit_events(false)
        .persist_results(true);
    debug_assert_eq!(config.orphaned_duration().as_secs(), orphan_window_secs());
    Ok(config)
}

pub async fn enqueue_job(job: Job, delay: Option<Duration>) -> Result<String> {
    job.validate()?;
    let message_id = job.message_id.clone();
    let queue = job.queue.clone();
    let mut builder = TaskBuilder::new(job).max_attempts(1);
    if let Some(delay) = delay.filter(|delay| !delay.is_zero()) {
        builder = builder.run_after(delay);
    }
    let mut storage = RedisStorage::<Job>::new(connection()?).with_config(queue_config(&queue)?);
    storage
        .push_task(builder.build())
        .await
        .map_err(|error| anyhow::anyhow!("enqueue failed: {error}"))?;
    Ok(message_id)
}

pub fn delivery_key(message_id: &str, retries: u32) -> String {
    format!("delivery:{message_id}:{retries}")
}

/// `INCR` and `EXPIRE` in one script. A crash between two commands used to
/// leave the key with no TTL.
const INCR_EXPIRE: &str = r#"
local n = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return n
"#;

fn delivery_counter() -> &'static redis::Script {
    static SCRIPT: OnceLock<redis::Script> = OnceLock::new();
    SCRIPT.get_or_init(|| redis::Script::new(INCR_EXPIRE))
}

/// Count a worker delivery. Orphan requeue does not call this.
///
/// The TTL is only the crash backstop. A finished ack deletes the key.
/// Poison and dead keep it, so a later delivery of the same attempt still
/// sees a count above `max_deliveries`.
pub async fn incr_delivery(message_id: &str, retries: u32) -> Result<u64> {
    let key = delivery_key(message_id, retries);
    let value: i64 = delivery_counter()
        .key(&key)
        .arg(names::DEFAULT_DEAD_TTL_SECS)
        .invoke_async(&mut connection()?)
        .await
        .context("failed to count a delivery")?;
    Ok(value as u64)
}

/// Drop the delivery key once this attempt is acked.
pub async fn release_delivery(message_id: &str, retries: u32) -> Result<()> {
    let key = delivery_key(message_id, retries);
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut connection()?)
        .await
        .context("failed to drop a delivery key")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_process_has_no_redis_connection() {
        assert!(!is_configured());
        assert!(connection().is_err());
    }

    #[test]
    fn delivery_keys_are_outside_the_queue_keyspace() {
        let key = delivery_key("m-1", 2);
        assert_eq!(key, "delivery:m-1:2");
        assert!(!key.contains(":done"));
        assert!(!key.contains(":inflight"));
        assert!(!key.ends_with(".DQ"));
    }

    #[test]
    fn the_delivery_script_increments_and_expires_together() {
        assert!(INCR_EXPIRE.contains("INCR"));
        assert!(INCR_EXPIRE.contains("EXPIRE"));
        assert_eq!(INCR_EXPIRE.matches("redis.call").count(), 2);
    }
}
