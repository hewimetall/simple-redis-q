//! Server side. A Redis lock whose key has already been checked by `names`.
//!
//! The business rule for a usable key lives in `names`. This module only
//! stores and releases the lock.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use redis::Script;

use redis::aio::ConnectionManager;

use crate::ids::lock_owner;
use crate::names::lock_redis_key;

static GUARDS: OnceLock<Mutex<HashMap<String, Arc<AtomicBool>>>> = OnceLock::new();
static LOOP_HEALTHY: AtomicBool = AtomicBool::new(true);
static OWNERS: AtomicU64 = AtomicU64::new(0);

const RENEW: &str = r#"
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
"#;

const RELEASE: &str = r#"
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"#;

fn guards() -> &'static Mutex<HashMap<String, Arc<AtomicBool>>> {
    GUARDS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn set_loop_healthy(healthy: bool) {
    LOOP_HEALTHY.store(healthy, Ordering::SeqCst);
}

pub fn loop_healthy() -> bool {
    LOOP_HEALTHY.load(Ordering::SeqCst)
}

/// One ping from the child.
///
/// A miss keeps the previous flag. Renewal stops only when the ping budget
/// has elapsed.
pub fn note_ping(ok: bool, budget_elapsed: bool) {
    set_loop_healthy(health_after_ping(loop_healthy(), ok, budget_elapsed));
}

pub(crate) fn health_after_ping(currently: bool, ok: bool, budget_elapsed: bool) -> bool {
    if budget_elapsed {
        false
    } else if ok {
        true
    } else {
        currently
    }
}

/// Extend only while this process still holds the owner and the ping budget
/// has not elapsed. A missed ping leaves `healthy` true.
pub(crate) fn should_extend(alive: bool, healthy: bool) -> bool {
    alive && healthy
}

/// Redis said the key is no longer ours. A transport error is not that, and
/// the next period tries again.
pub(crate) fn renew_lost(renewed: Result<bool, ()>) -> bool {
    matches!(renewed, Ok(false))
}

pub async fn acquire(
    mut conn: ConnectionManager,
    lock_key: String,
    ttl_seconds: u64,
) -> Result<Option<String>> {
    if ttl_seconds == 0 {
        anyhow::bail!("lock_ttl must be greater than zero");
    }
    let redis_key = lock_redis_key(&lock_key)?;
    let owner = lock_owner();
    OWNERS.fetch_add(1, Ordering::Relaxed);
    let ttl_ms = ttl_seconds.saturating_mul(1000);

    let acquired: Option<String> = redis::cmd("SET")
        .arg(&redis_key)
        .arg(&owner)
        .arg("NX")
        .arg("PX")
        .arg(ttl_ms)
        .query_async(&mut conn)
        .await
        .context("failed to acquire task lock")?;

    if acquired.is_none() {
        return Ok(None);
    }

    let alive = Arc::new(AtomicBool::new(true));
    guards()
        .lock()
        .expect("lock guards")
        .insert(owner.clone(), Arc::clone(&alive));

    let mut held = Hold {
        alive: Arc::clone(&alive),
        armed: true,
        pending: Some(PendingRelease {
            conn: conn.clone(),
            redis_key: redis_key.clone(),
            owner: owner.clone(),
        }),
    };

    let renew_key = redis_key;
    let renew_owner = owner.clone();
    let renew_conn = conn;
    tokio::spawn(async move {
        let period = Duration::from_millis((ttl_ms / 3).max(200));
        loop {
            tokio::time::sleep(period).await;
            let alive_now = alive.load(Ordering::SeqCst);
            if !alive_now {
                break;
            }
            // Unhealthy means the ping budget elapsed. A miss does not.
            if !should_extend(alive_now, loop_healthy()) {
                continue;
            }
            let renewed = renew(renew_conn.clone(), &renew_key, &renew_owner, ttl_ms)
                .await
                .map_err(|_| ());
            if renew_lost(renewed) {
                break;
            }
        }
    });

    // Hand the owner to Python. Until here, dropping this future stops
    // renewal and deletes the key.
    held.disarm();
    Ok(Some(owner))
}

pub async fn release(conn: ConnectionManager, lock_key: String, owner: String) -> Result<bool> {
    if let Some(flag) = guards().lock().expect("lock guards").remove(&owner) {
        flag.store(false, Ordering::SeqCst);
    }
    let redis_key = lock_redis_key(&lock_key)?;
    delete_if_owner(conn, redis_key, owner).await
}

struct PendingRelease {
    conn: ConnectionManager,
    redis_key: String,
    owner: String,
}

struct Hold {
    alive: Arc<AtomicBool>,
    armed: bool,
    pending: Option<PendingRelease>,
}

impl Hold {
    fn disarm(&mut self) {
        self.armed = false;
        self.pending.take();
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.alive.store(false, Ordering::SeqCst);
        let Some(pending) = self.pending.take() else {
            return;
        };
        if let Some(flag) = guards().lock().expect("lock guards").remove(&pending.owner) {
            flag.store(false, Ordering::SeqCst);
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = delete_if_owner(pending.conn, pending.redis_key, pending.owner).await;
            });
        }
    }
}

async fn delete_if_owner(
    mut conn: ConnectionManager,
    redis_key: String,
    owner: String,
) -> Result<bool> {
    let released: i32 = Script::new(RELEASE)
        .key(redis_key)
        .arg(owner)
        .invoke_async(&mut conn)
        .await
        .context("failed to release task lock")?;
    Ok(released == 1)
}

async fn renew(
    mut conn: ConnectionManager,
    redis_key: &str,
    owner: &str,
    ttl_ms: u64,
) -> Result<bool> {
    let renewed: i32 = Script::new(RENEW)
        .key(redis_key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(&mut conn)
        .await
        .context("failed to renew task lock")?;
    Ok(renewed == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missed_ping_keeps_renewal_until_the_budget_elapses() {
        assert!(health_after_ping(true, false, false));
        assert!(!health_after_ping(true, false, true));
        assert!(health_after_ping(false, true, false));
        assert!(!health_after_ping(true, true, true));
        assert!(should_extend(true, true));
        assert!(!should_extend(true, false));
        assert!(!should_extend(false, true));
    }

    #[test]
    fn a_redis_error_does_not_count_as_losing_the_lock() {
        assert!(!renew_lost(Ok(true)));
        assert!(renew_lost(Ok(false)));
        assert!(!renew_lost(Err(())));
    }

    #[test]
    fn dropping_an_acquire_before_python_sees_the_owner_stops_renewal() {
        let alive = Arc::new(AtomicBool::new(true));
        let held = Hold {
            alive: Arc::clone(&alive),
            armed: true,
            pending: None,
        };
        drop(held);
        assert!(!alive.load(Ordering::SeqCst));

        let alive = Arc::new(AtomicBool::new(true));
        let mut held = Hold {
            alive: Arc::clone(&alive),
            armed: true,
            pending: None,
        };
        held.disarm();
        drop(held);
        assert!(alive.load(Ordering::SeqCst));
    }
}
