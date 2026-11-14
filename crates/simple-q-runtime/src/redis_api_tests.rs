use super::*;

#[test]
fn the_idempotency_wait_doubles_until_five_seconds() {
    // Arrange

    let floor = Duration::from_millis(20);
    // Act

    let ceiling = Duration::from_secs(5);
    // Assert

    assert_eq!(
        next_idempotency_wait(floor, floor, ceiling),
        Duration::from_millis(40)
    );
    assert_eq!(
        next_idempotency_wait(Duration::from_secs(3), floor, ceiling),
        ceiling
    );
    assert_eq!(next_idempotency_wait(ceiling, floor, ceiling), ceiling);
}

#[test]
fn a_score_inside_the_window_is_alive_and_an_older_or_missing_score_is_not() {
    // Arrange

    let now = 1_700_000_100;
    // Act

    let window = 60;
    // Assert

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
    // Arrange
    // Act
    // Assert

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
    // Arrange

    let key = delivery_key("m-1", 2);
    // Act

    // Assert

    assert_eq!(key, "delivery:m-1:2");
    assert!(!key.contains(":done"));
    assert!(!key.contains(":inflight"));
    assert!(!key.ends_with(".DQ"));
}

#[test]
fn the_delivery_script_increments_and_expires_together() {
    // Arrange
    // Act
    // Assert

    assert!(INCR_EXPIRE.contains("INCR"));
    assert!(INCR_EXPIRE.contains("EXPIRE"));
    assert_eq!(INCR_EXPIRE.matches("redis.call").count(), 2);
}

#[test]
fn a_delayed_retry_holds_the_key_for_the_wait_plus_the_backstop() {
    // Arrange

    let ttl = delayed_retry_ttl_secs(Duration::from_secs(5), 2);
    // Act

    // Assert

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
    // Arrange

    let settings = Config::defaults();
    // Act

    let config = queue_config("default", &settings).unwrap();
    // Assert

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
