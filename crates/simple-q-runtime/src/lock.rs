use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use redis::Script;

use crate::ids::lock_owner;
use crate::names::lock_redis_key;
use crate::redis_api::connection;

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

pub async fn acquire(lock_key: String, ttl_seconds: u64) -> Result<Option<String>> {
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
        .query_async(&mut connection()?)
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

    let renew_key = redis_key.clone();
    let renew_owner = owner.clone();
    tokio::spawn(async move {
        let period = Duration::from_millis((ttl_ms / 3).max(200));
        while alive.load(Ordering::SeqCst) {
            tokio::time::sleep(period).await;
            if !alive.load(Ordering::SeqCst) || !loop_healthy() {
                break;
            }
            match renew(&renew_key, &renew_owner, ttl_ms).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => break,
            }
        }
    });

    Ok(Some(owner))
}

pub async fn release(lock_key: String, owner: String) -> Result<bool> {
    if let Some(flag) = guards().lock().expect("lock guards").remove(&owner) {
        flag.store(false, Ordering::SeqCst);
    }
    let redis_key = lock_redis_key(&lock_key)?;
    let released: i32 = Script::new(RELEASE)
        .key(redis_key)
        .arg(owner)
        .invoke_async(&mut connection()?)
        .await
        .context("failed to release task lock")?;
    Ok(released == 1)
}

async fn renew(redis_key: &str, owner: &str, ttl_ms: u64) -> Result<bool> {
    let renewed: i32 = Script::new(RENEW)
        .key(redis_key)
        .arg(owner)
        .arg(ttl_ms)
        .invoke_async(&mut connection()?)
        .await
        .context("failed to renew task lock")?;
    Ok(renewed == 1)
}
