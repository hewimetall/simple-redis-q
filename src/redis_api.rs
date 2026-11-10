use std::sync::OnceLock;

use anyhow::{Context, Result};
use apalis::prelude::{TaskBuilder, TaskSink};
use apalis_redis::{Config, RedisStorage, connect};
use redis::aio::ConnectionManager;

use crate::job::Job;
use crate::names::{
    HEARTBEAT_INTERVAL_SECS, MISSED_HEARTBEATS, orphan_window_secs, validate_queue,
};

static CONNECTION: OnceLock<ConnectionManager> = OnceLock::new();

pub async fn install_from_env() -> Result<()> {
    let url = std::env::var("REDIS_URL").context("REDIS_URL is required")?;
    let connection = connect(url).await.context("failed to connect to Redis")?;
    CONNECTION
        .set(connection)
        .map_err(|_| anyhow::anyhow!("Redis connection was already installed"))?;
    Ok(())
}

pub fn connection() -> Result<ConnectionManager> {
    CONNECTION
        .get()
        .cloned()
        .context("Redis connection is not installed")
}

pub fn queue_config(queue: &str) -> Result<Config> {
    validate_queue(queue)?;
    let config = Config::default()
        .queue(queue)
        .batch_size(1)
        .lock_tasks(true)
        .heartbeat_interval(std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS))
        .missed_heartbeats(MISSED_HEARTBEATS)
        .emit_events(false)
        .persist_results(true);
    debug_assert_eq!(config.orphaned_duration().as_secs(), orphan_window_secs());
    Ok(config)
}

/// Push one message. A Redis failure is returned; nothing is buffered in the process.
pub async fn enqueue(queue: String, task: String, payload_json: String) -> Result<()> {
    validate_queue(&queue)?;
    if task.is_empty() {
        anyhow::bail!("task name must not be empty");
    }
    let payload: serde_json::Value =
        serde_json::from_str(&payload_json).context("task payload must be JSON")?;
    if !payload.is_object() {
        anyhow::bail!("task payload must be a JSON object");
    }

    let job = Job { task, payload };
    let message = TaskBuilder::new(job).max_attempts(1).build();
    let mut storage = RedisStorage::<Job>::new(connection()?).with_config(queue_config(&queue)?);
    storage
        .push_task(message)
        .await
        .map_err(|error| anyhow::anyhow!("enqueue failed: {error}"))?;
    Ok(())
}
