//! Sends a failed worker command again when redis-rs says the caller should.
//!
//! `ConnectionManager` reconnects after a dropped connection and returns the
//! command that was in flight. It does not run that command again, because
//! some commands are not safe to repeat. The cluster client uses
//! `RedisError::retry_method` and retries inside the library. This is that
//! check for the standalone connection the worker uses.
//!
//! Fetch and enqueue are not repeated. A lost fetch reply has already moved
//! the task into this worker's inflight set, and a second fetch does not hand
//! it back. Repeating it would leave the task there while this name kept
//! heartbeating. A repeated enqueue would store a second task.

use std::time::Duration;

use redis::aio::{ConnectionLike, ConnectionManager};
use redis::{Cmd, Pipeline, RedisError, RedisFuture, RetryMethod, Value};

/// How long a retryable command is sent again before the error is returned.
///
/// Two minutes is longer than the default orphan window (60 seconds), so an
/// ack can still land when Redis accepts commands again. It is finite so a
/// Redis that stays down cannot pin the process inside one command.
const REPLAY_FOR: Duration = Duration::from_secs(120);

/// apalis-redis 1.1.1 `fetch_next.lua`, as a RESP bulk string.
const FETCH_SHA: &[u8] = b"$40\r\nf053fd9b41b61a2d32b8b7d8dba3f1a96e3aa6e1\r\n";
/// apalis-redis 1.1.1 `batch_push.lua`, as a RESP bulk string.
const ENQUEUE_SHA: &[u8] = b"$40\r\n3db3e4f4b96d92c5dd8874e33f1dd4c0152c0682\r\n";

/// Worker Redis connection. Heartbeat, renewal, and ack are sent again.
/// Fetch and enqueue are not.
#[derive(Clone)]
pub struct RetryingConnection {
    inner: ConnectionManager,
}

impl RetryingConnection {
    pub fn new(inner: ConnectionManager) -> Self {
        Self { inner }
    }
}

impl ConnectionLike for RetryingConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let packed = cmd.get_packed_command();
        Box::pin(async move {
            let started = std::time::Instant::now();
            let mut attempt = 0u32;
            loop {
                match self.inner.req_packed_command(cmd).await {
                    Ok(value) => return Ok(value),
                    Err(error) if replay(&packed, &error) && started.elapsed() < REPLAY_FOR => {
                        let pause = pause_for(error.retry_method(), attempt);
                        attempt = attempt.saturating_add(1);
                        tokio::time::sleep(pause).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        let packed = pipeline.get_packed_pipeline();
        Box::pin(async move {
            let started = std::time::Instant::now();
            let mut attempt = 0u32;
            loop {
                match self
                    .inner
                    .req_packed_commands(pipeline, offset, count)
                    .await
                {
                    Ok(value) => return Ok(value),
                    Err(error) if replay(&packed, &error) && started.elapsed() < REPLAY_FOR => {
                        let pause = pause_for(error.retry_method(), attempt);
                        attempt = attempt.saturating_add(1);
                        tokio::time::sleep(pause).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        })
    }

    fn get_db(&self) -> i64 {
        self.inner.get_db()
    }
}

fn replay(packed: &[u8], error: &RedisError) -> bool {
    if claims_on_success(packed) {
        return false;
    }
    matches!(
        error.retry_method(),
        RetryMethod::Reconnect | RetryMethod::RetryImmediately | RetryMethod::WaitAndRetry
    )
}

/// True for the scripts that store a new claim or a new task.
///
/// The hash is the steady-state `EVALSHA`. The text is the first `EVAL` or
/// `SCRIPT LOAD`, before Redis has cached the script.
fn claims_on_success(packed: &[u8]) -> bool {
    if contains_ignore_ascii_case(packed, FETCH_SHA)
        || contains_ignore_ascii_case(packed, ENQUEUE_SHA)
    {
        return true;
    }
    let eval = contains_ignore_ascii_case(packed, b"$4\r\nEVAL\r\n");
    let script = contains_ignore_ascii_case(packed, b"$6\r\nSCRIPT\r\n");
    if !eval && !script {
        return false;
    }
    memmem(packed, b"Cant fetch next") || memmem(packed, b"the idempotency_ttl")
}

fn contains_ignore_ascii_case(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn memmem(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.len() >= needle.len()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn pause_for(method: RetryMethod, attempt: u32) -> Duration {
    let base_ms: u64 = match method {
        RetryMethod::RetryImmediately => 20,
        _ => 50,
    };
    let shift = attempt.min(6);
    let millis = base_ms.saturating_mul(1u64 << shift).min(1_000);
    Duration::from_millis(millis)
}

#[cfg(test)]
#[path = "redis_retry_tests.rs"]
mod tests;
