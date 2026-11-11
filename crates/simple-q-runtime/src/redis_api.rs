//! Server side. Opens Redis connections that callers pass in.
//!
//! Importing the Python module does not call `connect`.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use apalis::prelude::{TaskBuilder, TaskSink};
use apalis_redis::{Config as QueueConfig, RedisStorage, connect as connect_redis, queries};
pub use redis::aio::ConnectionManager;

use crate::config::Config;
use crate::job::Job;
use crate::names::{self, WORKER_NAME_PREFIX, validate_queue};

pub async fn connect(url: &str) -> Result<ConnectionManager> {
    connect_redis(url.to_owned())
        .await
        .context("failed to connect to Redis")
}

pub fn queue_config(queue: &str, settings: &Config) -> Result<QueueConfig> {
    validate_queue(queue)?;
    // apalis 1.0.0 uses this flag as the ack switch. Turning it off leaves the
    // message inflight. Finished payloads are removed by the sweeper instead.
    let config = QueueConfig::default()
        .queue(queue)
        .batch_size(1)
        .lock_tasks(true)
        .heartbeat_interval(Duration::from_secs(settings.heartbeat_secs))
        .missed_heartbeats(settings.missed_heartbeats as usize)
        .emit_events(false)
        .persist_results(true);
    debug_assert_eq!(
        config.orphaned_duration().as_secs(),
        settings.orphan_window_secs()
    );
    Ok(config)
}

/// Queue encoded in a worker name.
///
/// Names are `{prefix}.{queue}.{hostname}.{pid}.{suffix}`. The prefix is
/// [`WORKER_NAME_PREFIX`]. The queue is one lowercase word, so it is the
/// second field even when the hostname contains dots.
pub fn queue_of_worker_name(name: &str) -> Option<&str> {
    let mut fields = name.split('.');
    if fields.next() != Some(WORKER_NAME_PREFIX) {
        return None;
    }
    let queue = fields.next()?;
    if fields.next().is_none() || fields.next().is_none() || fields.next().is_none() {
        return None;
    }
    validate_queue(queue).ok()?;
    Some(queue)
}

/// A heartbeat score is alive when it is still inside the window.
///
/// `score` is unix seconds from `ZSCORE`, or missing when the member is absent.
/// Alive means the score is present and `score >= now_secs - window_secs`.
pub fn heartbeat_is_fresh(score: Option<f64>, now_secs: u64, window_secs: u64) -> bool {
    let Some(score) = score else {
        return false;
    };
    score.is_finite() && score >= now_secs.saturating_sub(window_secs) as f64
}

/// Whether `name` still has a fresh entry in `{queue}:workers`.
///
/// The clock is Redis `TIME`. The window is this config's
/// `heartbeat_secs * missed_heartbeats`. An unparseable name or a missing
/// connection is not alive. Neither case is logged.
pub async fn worker_alive(
    conn: Option<&mut ConnectionManager>,
    settings: &Config,
    name: &str,
) -> Result<bool> {
    let Some(queue) = queue_of_worker_name(name) else {
        return Ok(false);
    };
    let Some(conn) = conn else {
        return Ok(false);
    };
    let score: Option<f64> = redis::cmd("ZSCORE")
        .arg(format!("{queue}:workers"))
        .arg(name)
        .query_async(conn)
        .await
        .context("failed to read a worker heartbeat")?;
    let (now_secs, _micros): (u64, u64) = redis::cmd("TIME")
        .query_async(conn)
        .await
        .context("failed to read Redis time")?;
    Ok(heartbeat_is_fresh(
        score,
        now_secs,
        settings.orphan_window_secs(),
    ))
}

/// Redis string written by the apalis-redis idempotency script.
///
/// The script stores `SETNX {queue}:idempotency:{key}`. That is
/// `Config::idempotency_key_set()` plus `:` plus the key.
pub fn idempotency_redis_key(queue: &str, unique_key: &str) -> String {
    format!("{queue}:idempotency:{unique_key}")
}

/// A first send claims the key. A retry must not: the attempt still in
/// progress already holds it, and claiming again would drop the re-enqueue.
pub fn claims_idempotency(job: &Job) -> bool {
    job.retries == 0 && job.unique_key.as_deref().is_some_and(|key| !key.is_empty())
}

/// TTL written on a delayed retry: `delay + unique_ttl`.
///
/// The TTL stays a backstop. It must not expire while the retry is waiting.
/// Zero is not a TTL. The idempotency script treats the string `"0"` as
/// present and `EXPIRE 0` deletes the key immediately.
pub fn delayed_retry_ttl_secs(delay: Duration, unique_ttl_secs: u64) -> u64 {
    let delay_secs = delay.as_secs() + u64::from(delay.subsec_nanos() > 0);
    delay_secs.saturating_add(unique_ttl_secs).max(1)
}

/// Enqueue `job`.
///
/// `None` means a `unique_key` was already held, so nothing was queued.
/// Sends without a key, and retries, always return the message id.
pub async fn enqueue_job(
    conn: &mut ConnectionManager,
    settings: &Config,
    job: Job,
    delay: Option<Duration>,
) -> Result<Option<String>> {
    job.validate()?;
    extend_idempotency_for_delay(conn, settings, &job, delay).await?;
    if claims_idempotency(&job) {
        enqueue_unique(conn, settings, job, delay).await
    } else {
        enqueue_plain(conn, settings, job, delay).await.map(Some)
    }
}

async fn extend_idempotency_for_delay(
    conn: &mut ConnectionManager,
    settings: &Config,
    job: &Job,
    delay: Option<Duration>,
) -> Result<()> {
    if job.retries == 0 {
        return Ok(());
    }
    let Some(unique_key) = job.unique_key.as_deref().filter(|key| !key.is_empty()) else {
        return Ok(());
    };
    let Some(delay) = delay.filter(|delay| !delay.is_zero()) else {
        return Ok(());
    };
    let ttl = delayed_retry_ttl_secs(delay, settings.unique_ttl_secs);
    let key = idempotency_redis_key(&job.queue, unique_key);
    let _: i64 = redis::cmd("EXPIRE")
        .arg(&key)
        .arg(ttl)
        .query_async(conn)
        .await
        .context("failed to extend an idempotency key")?;
    Ok(())
}

async fn enqueue_plain(
    conn: &mut ConnectionManager,
    settings: &Config,
    job: Job,
    delay: Option<Duration>,
) -> Result<String> {
    let message_id = job.message_id.clone();
    let queue = job.queue.clone();
    let mut builder = TaskBuilder::new(job).max_attempts(1);
    if let Some(delay) = delay.filter(|delay| !delay.is_zero()) {
        builder = builder.run_after(delay);
    }
    let mut storage =
        RedisStorage::<Job>::new(conn.clone()).with_config(queue_config(&queue, settings)?);
    storage
        .push_task(builder.build())
        .await
        .map_err(|error| anyhow::anyhow!("enqueue failed: {error}"))?;
    Ok(message_id)
}

async fn enqueue_unique(
    conn: &mut ConnectionManager,
    settings: &Config,
    job: Job,
    delay: Option<Duration>,
) -> Result<Option<String>> {
    let message_id = job.message_id.clone();
    let queue = job.queue.clone();
    let unique_key = job
        .unique_key
        .clone()
        .filter(|key| !key.is_empty())
        .context("unique enqueue requires unique_key")?;
    // RedisStorage encodes with JsonCodec<Vec<u8>>, which is serde_json::to_vec.
    // The worker decodes those bytes back into Job. Retries still carry the
    // string on the job; only the task builder's idempotency key is omitted.
    let claim = job.retries == 0;
    let bytes = serde_json::to_vec(&job).context("failed to encode the job")?;
    let mut builder = TaskBuilder::new(bytes).max_attempts(1);
    if let Some(delay) = delay.filter(|delay| !delay.is_zero()) {
        builder = builder.run_after(delay);
    }
    if claim {
        builder = builder.idempotency_key(unique_key);
    }
    // A missing TTL is passed to Lua as 0. The script treats the string "0"
    // as present and EXPIRE 0 deletes the key, so the TTL is always positive.
    let config = queue_config(&queue, settings)?
        .idempotency_ttl(Some(Duration::from_secs(settings.unique_ttl_secs)));
    let (enqueued, scheduled) = queries::push_tasks(conn, &config, &[builder.build()])
        .await
        .map_err(|error| anyhow::anyhow!("enqueue failed: {error}"))?;
    if enqueued == 0 && scheduled == 0 {
        return Ok(None);
    }
    Ok(Some(message_id))
}

/// `true` when `task_id` is already in `{queue}:done` or `{queue}:dead`.
///
/// Either key may be a set or a sorted set. A missing key is not terminal.
pub async fn task_is_terminal(
    conn: &mut ConnectionManager,
    queue: &str,
    task_id: &str,
) -> Result<bool> {
    if terminal_member(conn, &format!("{queue}:done"), task_id).await? {
        return Ok(true);
    }
    terminal_member(conn, &format!("{queue}:dead"), task_id).await
}

async fn terminal_member(conn: &mut ConnectionManager, key: &str, task_id: &str) -> Result<bool> {
    let kind: String = redis::cmd("TYPE")
        .arg(key)
        .query_async(conn)
        .await
        .with_context(|| format!("failed to read the type of {key}"))?;
    match kind.as_str() {
        "none" => Ok(false),
        "set" => {
            let present: i64 = redis::cmd("SISMEMBER")
                .arg(key)
                .arg(task_id)
                .query_async(conn)
                .await
                .with_context(|| format!("failed to read {key}"))?;
            Ok(present == 1)
        }
        "zset" => {
            let score: Option<String> = redis::cmd("ZSCORE")
                .arg(key)
                .arg(task_id)
                .query_async(conn)
                .await
                .with_context(|| format!("failed to read {key}"))?;
            Ok(score.is_some())
        }
        other => bail!("{key} has type {other}"),
    }
}

/// Drop `{queue}:idempotency:{unique_key}` after a terminal outcome.
pub async fn release_idempotency(
    conn: &mut ConnectionManager,
    queue: &str,
    unique_key: &str,
) -> Result<()> {
    if unique_key.is_empty() {
        return Ok(());
    }
    let key = idempotency_redis_key(queue, unique_key);
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(conn)
        .await
        .context("failed to drop an idempotency key")?;
    Ok(())
}

pub fn delivery_key(message_id: &str, retries: u32) -> String {
    format!("delivery:{message_id}:{retries}")
}

/// `INCR` and `EXPIRE` run in one script, so the key always receives a TTL.
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
pub async fn incr_delivery(
    conn: &mut ConnectionManager,
    message_id: &str,
    retries: u32,
) -> Result<u64> {
    let key = delivery_key(message_id, retries);
    let value: i64 = delivery_counter()
        .key(&key)
        .arg(names::DEFAULT_DEAD_TTL_SECS)
        .invoke_async(conn)
        .await
        .context("failed to count a delivery")?;
    Ok(value as u64)
}

/// Drop the delivery key once this attempt is acked.
pub async fn release_delivery(
    conn: &mut ConnectionManager,
    message_id: &str,
    retries: u32,
) -> Result<()> {
    let key = delivery_key(message_id, retries);
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(conn)
        .await
        .context("failed to drop a delivery key")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_score_inside_the_window_is_alive_and_an_older_or_missing_score_is_not() {
        let now = 1_700_000_100;
        let window = 60;
        assert!(heartbeat_is_fresh(Some(now as f64), now, window));
        assert!(heartbeat_is_fresh(
            Some(now.saturating_sub(window) as f64),
            now,
            window
        ));
        assert!(!heartbeat_is_fresh(
            Some(now.saturating_sub(window + 1) as f64),
            now,
            window
        ));
        assert!(!heartbeat_is_fresh(None, now, window));
    }

    #[test]
    fn the_queue_is_the_second_field_of_a_worker_name() {
        assert_eq!(
            queue_of_worker_name("agent-worker.default.localhost.7.abcd1234"),
            Some("default")
        );
        assert_eq!(
            queue_of_worker_name("agent-worker.holdq.host.example.com.7.abcd1234"),
            Some("holdq")
        );
        assert_eq!(queue_of_worker_name("agent-worker.default.host.7"), None);
        assert_eq!(queue_of_worker_name("worker.default.host.7.abcd1234"), None);
        assert_eq!(
            queue_of_worker_name("agent-worker.NotQueue.host.7.abcd1234"),
            None
        );
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

    #[test]
    fn a_delayed_retry_holds_the_key_for_the_wait_plus_the_backstop() {
        let ttl = delayed_retry_ttl_secs(Duration::from_secs(5), 2);
        assert_eq!(ttl, 7);
        assert!(ttl > 2);
        assert!(delayed_retry_ttl_secs(Duration::from_millis(1), 2) >= 3);
    }

    fn sample(retries: u32, unique_key: Option<&str>) -> Job {
        Job {
            version: crate::MESSAGE_VERSION,
            message_id: "m-1".to_owned(),
            actor_name: "recalc".to_owned(),
            queue: "default".to_owned(),
            kwargs: serde_json::json!({"project": "42"}),
            headers: Default::default(),
            retries,
            max_retries: 2,
            time_limit_ms: 1_800_000,
            min_backoff_ms: 15_000,
            max_backoff_ms: 86_400_000,
            max_deliveries: 5,
            lock_key: None,
            lock_ttl: 30,
            skip_on_lock: false,
            unique_key: unique_key.map(str::to_owned),
        }
    }

    #[test]
    fn the_idempotency_key_matches_the_apalis_set_and_a_retry_does_not_claim_it() {
        let settings = Config::defaults();
        let config = queue_config("default", &settings).unwrap();
        assert_eq!(
            idempotency_redis_key("default", "recalc:42"),
            format!("{}:recalc:42", config.idempotency_key_set())
        );
        assert!(config.idempotency_ttl.is_none());
        let ttl = config
            .idempotency_ttl(Some(Duration::from_secs(settings.unique_ttl_secs)))
            .idempotency_ttl
            .expect("ttl")
            .as_secs();
        assert!(ttl > 0);

        assert!(claims_idempotency(&sample(0, Some("recalc:42"))));
        assert!(!claims_idempotency(&sample(1, Some("recalc:42"))));
        assert!(!claims_idempotency(&sample(0, None)));
        assert!(!claims_idempotency(&sample(0, Some(""))));
    }
}
