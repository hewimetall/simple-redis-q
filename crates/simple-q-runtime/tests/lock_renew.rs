//! Redis lock acquire, renewal, and release.

use std::time::Duration;

use simple_q_runtime::lock;
use simple_q_runtime::names::lock_redis_key;
use simple_q_runtime::redis_api::{self, ConnectionManager};

const REDIS_URL: &str = "redis://127.0.0.1:6379/15";

fn key(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{}-{nanos}", std::process::id())
}

async fn del(conn: &mut ConnectionManager, redis_key: &str) {
    let _: Result<i64, _> = redis::cmd("DEL").arg(redis_key).query_async(conn).await;
}

#[tokio::test]
async fn given_a_zero_ttl_or_a_prefixed_key_when_acquire_runs_then_it_fails() {
    // Arrange
    let Ok(conn) = redis_api::connect(REDIS_URL).await else {
        return;
    };

    // Act
    let zero = lock::acquire(conn.clone(), "demo:gate".to_owned(), 0).await;
    let prefixed = lock::acquire(conn, "lock:demo".to_owned(), 1).await;

    // Assert
    assert!(zero.unwrap_err().to_string().contains("greater than zero"));
    assert!(prefixed.unwrap_err().to_string().contains("lock_key"));
}

#[tokio::test]
async fn given_a_held_key_when_released_by_the_owner_then_the_key_is_gone() {
    // Arrange
    let Ok(mut conn) = redis_api::connect(REDIS_URL).await else {
        return;
    };
    let lock_key = key("held");
    let redis_key = lock_redis_key(&lock_key).unwrap();
    let owner = lock::acquire(conn.clone(), lock_key.clone(), 30)
        .await
        .unwrap()
        .expect("owner");

    // Act
    let busy = lock::acquire(conn.clone(), lock_key.clone(), 30)
        .await
        .unwrap();
    let wrong = lock::release(conn.clone(), lock_key.clone(), "other-owner".to_owned())
        .await
        .unwrap();
    let released = lock::release(conn.clone(), lock_key, owner).await.unwrap();
    let exists: i64 = redis::cmd("EXISTS")
        .arg(&redis_key)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Assert
    assert!(busy.is_none());
    assert!(!wrong);
    assert!(released);
    assert_eq!(exists, 0);
    del(&mut conn, &redis_key).await;
}

#[tokio::test]
async fn given_a_held_lock_when_the_ping_budget_and_a_deleted_key_arrive_then_renewal_stops() {
    // Arrange
    let Ok(mut conn) = redis_api::connect(REDIS_URL).await else {
        return;
    };
    lock::set_loop_healthy(true);
    let lock_key = key("renew");
    let redis_key = lock_redis_key(&lock_key).unwrap();
    let owner = lock::acquire(conn.clone(), lock_key.clone(), 3)
        .await
        .unwrap()
        .expect("owner");
    lock::set_loop_healthy(false);
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    lock::set_loop_healthy(true);
    let before: i64 = redis::cmd("PTTL")
        .arg(&redis_key)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Act
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let after: i64 = redis::cmd("PTTL")
        .arg(&redis_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(&redis_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let released = lock::release(conn.clone(), lock_key, owner).await.unwrap();

    // Assert
    assert!(after > before, "pttl before {before} after {after}");
    assert!(!released);
    assert!(lock::loop_healthy());
    del(&mut conn, &redis_key).await;
}
