use super::*;

#[test]
fn a_queue_name_is_one_lowercase_word() {
    // Arrange
    // Act
    // Assert

    assert!(validate_queue(DEFAULT_QUEUE).is_ok());
    assert!(validate_queue("holdq").is_ok());
    assert!(validate_queue("fan-out").is_err());
    assert!(validate_queue("fan_out").is_err());
    assert!(validate_queue("simple-q").is_err());
    assert!(validate_queue("").is_err());
    let error = validate_queue("Bad").unwrap_err().to_string();
    assert!(error.contains("default"), "{error}");
}

#[test]
fn lock_keys_keep_a_prefix_outside_the_apalis_keyspace() {
    // Arrange
    // Act
    // Assert

    assert_eq!(lock_redis_key("demo:gate").unwrap(), "lock:demo:gate");
    assert!(lock_redis_key("lock:demo:gate").is_err());
    assert!(lock_redis_key("").is_err());
    assert!(lock_redis_key("demo gate").is_err());
    assert!(lock_redis_key("demo:bad\u{200b}").is_err());
}

#[test]
fn orphan_window_is_inside_the_redelivery_bound() {
    // Arrange
    // Act
    // Assert

    assert_eq!(HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS as u64, 60);
    assert!(HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS as u64 <= 75);
}

#[test]
fn ping_budget_covers_the_default_time_limit_and_a_minute_block() {
    // Arrange
    // Act
    // Assert

    assert_eq!(DEFAULT_TIME_LIMIT_MS, 1_800_000);
    assert!(default_ping_budget() >= Duration::from_secs(30 * 60 + 60));
    assert!(default_ping_budget() > Duration::from_secs(60));
}

#[test]
fn dead_messages_are_kept_for_seven_days() {
    // Arrange
    // Act
    // Assert

    assert_eq!(DEFAULT_DEAD_TTL_SECS, 7 * 24 * 60 * 60);
}

#[test]
fn unique_keys_keep_a_positive_ttl() {
    // Arrange
    // Act
    // Assert

    // Zero is not a TTL. The idempotency script treats the string `"0"` as
    // present and `EXPIRE 0` deletes the key immediately.
    assert_eq!(DEFAULT_UNIQUE_TTL_SECS, 86_400);
}
