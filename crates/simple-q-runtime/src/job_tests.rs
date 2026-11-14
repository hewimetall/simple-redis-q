use super::*;
use serde_json::json;

fn sample() -> Value {
    json!({
        "version": 1,
        "message_id": "m-1",
        "actor_name": "discover",
        "queue": "default",
        "kwargs": {"value": 1},
        "headers": {"trace": "abc"},
        "retries": 0,
        "max_retries": 3,
        "time_limit_ms": 1_800_000,
        "min_backoff_ms": 15000,
        "max_backoff_ms": 60000,
        "max_deliveries": 5
    })
}

#[test]
fn version_one_round_trips_and_keeps_headers_out_of_kwargs() {
    // Arrange

    let job = Job::parse(sample()).unwrap();
    // Act

    // Assert

    assert_eq!(job.version, 1);
    assert_eq!(job.headers.get("trace").map(String::as_str), Some("abc"));
    assert!(job.kwargs.get("headers").is_none());
    assert!(job.kwargs.get("trace").is_none());
}

#[test]
fn a_payload_without_unique_key_still_parses_at_version_one() {
    // Arrange

    let job = Job::parse(sample()).unwrap();
    // Act

    // Assert

    assert_eq!(job.version, MESSAGE_VERSION);
    assert_eq!(job.unique_key, None);
    let encoded = serde_json::to_value(&job).unwrap();
    assert!(encoded.get("unique_key").is_none());

    let mut explicit_null = sample();
    explicit_null["unique_key"] = json!(null);
    assert_eq!(Job::parse(explicit_null).unwrap().unique_key, None);

    let mut held = sample();
    held["unique_key"] = json!("recalc:proj-42");
    let job = Job::parse(held).unwrap();
    assert_eq!(job.unique_key.as_deref(), Some("recalc:proj-42"));
    assert_eq!(job.version, MESSAGE_VERSION);
}

#[test]
fn a_missing_or_foreign_version_is_rejected() {
    // Arrange

    let mut missing = sample();
    // Act

    missing.as_object_mut().unwrap().remove("version");
    // Assert

    assert!(
        Job::parse(missing)
            .unwrap_err()
            .to_string()
            .contains("version")
    );

    let mut foreign = sample();
    foreign["version"] = json!(2);
    assert!(
        Job::parse(foreign)
            .unwrap_err()
            .to_string()
            .contains("unsupported message version")
    );
}

#[test]
fn a_lock_without_a_positive_ttl_is_rejected() {
    // Arrange

    let mut payload = sample();
    payload["lock_key"] = json!("demo:gate");
    // Act

    payload["lock_ttl"] = json!(0);
    // Assert

    assert!(
        Job::parse(payload)
            .unwrap_err()
            .to_string()
            .contains("lock_ttl")
    );

    let mut held = sample();
    held["lock_key"] = json!("demo:gate");
    held["lock_ttl"] = json!(30);
    let job = Job::parse(held).unwrap();
    assert_eq!(job.lock_key.as_deref(), Some("demo:gate"));
    assert_eq!(job.lock_ttl, 30);
}

#[test]
fn a_delivery_past_the_limit_is_poison_and_the_limit_itself_still_runs() {
    // Arrange
    // Act
    // Assert

    assert_eq!(decide(1, 1), Decision::Run);
    assert_eq!(decide(2, 1), Decision::Poison);
    assert_eq!(decide(5, 5), Decision::Run);
    assert_eq!(decide(6, 5), Decision::Poison);
}

#[test]
fn the_message_module_does_not_open_redis() {
    // Arrange

    let source = include_str!("job.rs"); // Act

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("use ") || trimmed.starts_with("extern ") {
            for crate_name in ["redis", "pyo3", "tokio", "apalis"] {
                assert!(
                    !trimmed.contains(crate_name),
                    "job.rs imports {crate_name}: {trimmed}"
                );
            }
        }
    } // Assert
}
