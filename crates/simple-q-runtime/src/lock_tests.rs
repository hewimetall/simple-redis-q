use std::time::Duration;

use super::*;

#[test]
fn a_missed_ping_keeps_renewal_until_the_budget_elapses() {
    // Arrange
    // Act
    // Assert

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
    // Arrange
    // Act
    // Assert

    assert!(!renew_lost(Ok(true)));
    assert!(renew_lost(Ok(false)));
    assert!(!renew_lost(Err(())));
}

#[test]
fn dropping_an_acquire_before_python_sees_the_owner_stops_renewal() {
    // Arrange

    let alive = Arc::new(AtomicBool::new(true));
    let held = Hold {
        alive: Arc::clone(&alive),
        armed: true,
        pending: None,
    };
    // Act

    drop(held);
    // Assert

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

#[test]
fn given_a_healthy_loop_when_the_ping_budget_elapses_then_renewal_stops() {
    // Arrange
    set_loop_healthy(true);

    // Act
    note_ping(false, false);
    let kept = loop_healthy();
    note_ping(false, true);
    let stopped = loop_healthy();
    note_ping(true, false);

    // Assert
    assert!(kept);
    assert!(!stopped);
    assert!(loop_healthy());
}

#[test]
fn given_an_armed_hold_when_it_drops_on_a_runtime_then_the_key_is_deleted() {
    // Arrange
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let Ok(mut conn) = crate::redis_api::connect("redis://127.0.0.1:6379/15").await else {
            return;
        };
        let key = format!("drop-{}", std::process::id());
        let redis_key = crate::names::lock_redis_key(&key).unwrap();
        let owner = format!("owner-{key}");
        let _: String = redis::cmd("SET")
            .arg(&redis_key)
            .arg(&owner)
            .arg("PX")
            .arg(30_000)
            .query_async(&mut conn)
            .await
            .unwrap();
        let alive = Arc::new(AtomicBool::new(true));
        guards()
            .lock()
            .expect("lock guards")
            .insert(owner.clone(), Arc::clone(&alive));
        let held = Hold {
            alive: Arc::clone(&alive),
            armed: true,
            pending: Some(PendingRelease {
                conn: conn.clone(),
                redis_key: redis_key.clone(),
                owner: owner.clone(),
            }),
        };

        // Act
        drop(held);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&redis_key)
            .query_async(&mut conn)
            .await
            .unwrap();

        // Assert
        assert!(!alive.load(Ordering::SeqCst));
        assert!(guards().lock().expect("lock guards").get(&owner).is_none());
        assert_eq!(exists, 0);
        let _: i64 = redis::cmd("DEL")
            .arg(&redis_key)
            .query_async(&mut conn)
            .await
            .unwrap();
    });
}
