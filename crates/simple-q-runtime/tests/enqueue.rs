use std::process::Command;
use std::time::Duration;

use simple_q_runtime::MESSAGE_VERSION;
use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::redis_api;

fn redis(args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .args(["-n", "15"])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn job(queue: &str, message_id: &str, retries: u32, unique_key: Option<&str>) -> Job {
    Job {
        version: MESSAGE_VERSION,
        message_id: message_id.to_owned(),
        actor_name: "recalc".to_owned(),
        queue: queue.to_owned(),
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

fn hlen(queue: &str) -> u64 {
    redis(&["HLEN", &format!("{queue}:data")])
        .parse()
        .unwrap_or(0)
}

fn del_queue(queue: &str) {
    for suffix in [
        "data",
        "active",
        "scheduled",
        "signal",
        "dead",
        "done",
        "failed",
        "workers",
        "inflight",
    ] {
        let key = format!("{queue}:{suffix}");
        let _ = redis(&["DEL", &key]);
    }
    for pattern in [
        format!("{queue}:meta:*"),
        format!("{queue}:idempotency:*"),
        format!("{queue}:inflight:*"),
    ] {
        let listed = redis(&["KEYS", &pattern]);
        if listed.is_empty() {
            continue;
        }
        for key in listed.split_whitespace() {
            let _ = redis(&["DEL", key]);
        }
    }
}

#[test]
fn idempotency_keys_live_beside_the_queue() {
    assert_eq!(
        redis_api::idempotency_redis_key("default", "recalc:proj-42"),
        "default:idempotency:recalc:proj-42"
    );
    let first = job("default", "m-1", 0, Some("recalc:proj-42"));
    let retry = job("default", "m-1", 1, Some("recalc:proj-42"));
    assert!(redis_api::claims_idempotency(&first));
    assert!(!redis_api::claims_idempotency(&retry));
    assert!(!redis_api::claims_idempotency(&job(
        "default", "m-1", 0, None
    )));
}

#[tokio::test]
async fn redis_enqueue_on_database_15() {
    // One runtime owns the connection. A second `tokio::test` would reuse the
    // process-wide client after its driver task had already stopped.
    let url = "redis://127.0.0.1:6379/15".to_owned();
    if redis_api::install(url).await.is_err() {
        return;
    }
    a_delayed_message_stays_on_its_own_queue().await;
    a_unique_key_is_held_until_a_terminal_delete().await;
    a_delayed_unique_task_is_placed_when_it_is_scheduled().await;
}

async fn a_delayed_message_stays_on_its_own_queue() {
    for key in [
        "qtest:scheduled",
        "qtest:data",
        "quota:scheduled",
        "quota:data",
    ] {
        let _ = redis(&["DEL", key]);
    }
    let message_id = new_message_id();
    let job = Job {
        version: MESSAGE_VERSION,
        message_id: message_id.clone(),
        actor_name: "discover".to_owned(),
        queue: "qtest".to_owned(),
        kwargs: serde_json::json!({"n": 1}),
        headers: Default::default(),
        retries: 0,
        max_retries: 2,
        time_limit_ms: 1_800_000,
        min_backoff_ms: 15_000,
        max_backoff_ms: 86_400_000,
        max_deliveries: 5,
        lock_key: None,
        lock_ttl: 30,
        skip_on_lock: false,
        unique_key: None,
    };
    redis_api::enqueue_job(job, Some(Duration::from_secs(30)))
        .await
        .expect("enqueue")
        .expect("queued");
    assert_eq!(redis(&["ZCARD", "qtest:scheduled"]), "1");
    assert_eq!(redis(&["ZCARD", "quota:scheduled"]), "0");
    let stored = redis(&["HVALS", "qtest:data"]);
    assert!(stored.contains(&message_id), "{stored}");
    assert_eq!(
        redis_api::incr_delivery(&message_id, 0)
            .await
            .expect("delivery"),
        1
    );
    let key = format!("delivery:{message_id}:0");
    assert_eq!(redis(&["GET", &key]), "1");
    let ttl = redis(&["TTL", &key]);
    assert!(ttl.parse::<i64>().expect("ttl") > 0, "{ttl}");
    redis_api::release_delivery(&message_id, 0)
        .await
        .expect("release");
    assert_eq!(redis(&["GET", &key]), "");
}

async fn a_unique_key_is_held_until_a_terminal_delete() {
    let queue = "uniq";
    let unique = "recalc:proj-42";
    del_queue(queue);
    let idem = redis_api::idempotency_redis_key(queue, unique);

    let first_id = new_message_id();
    let queued = redis_api::enqueue_job(job(queue, &first_id, 0, Some(unique)), None)
        .await
        .expect("enqueue");
    assert_eq!(queued.as_deref(), Some(first_id.as_str()));
    assert_eq!(hlen(queue), 1);
    let held = redis(&["GET", &idem]);
    assert!(!held.is_empty(), "idempotency key missing");
    let ttl: i64 = redis(&["TTL", &idem]).parse().expect("ttl");
    assert!(ttl > 0, "{ttl}");
    let stored = redis(&["HVALS", &format!("{queue}:data")]);
    assert!(stored.contains(&first_id), "{stored}");
    assert!(stored.contains(unique), "{stored}");

    let duplicate = redis_api::enqueue_job(job(queue, &new_message_id(), 0, Some(unique)), None)
        .await
        .expect("duplicate");
    assert_eq!(duplicate, None);
    assert_eq!(
        hlen(queue),
        1,
        "a held key must not add a second queue entry"
    );
    assert_eq!(redis(&["GET", &idem]), held);

    let retry = redis_api::enqueue_job(job(queue, &first_id, 1, Some(unique)), None)
        .await
        .expect("retry");
    assert_eq!(retry.as_deref(), Some(first_id.as_str()));
    assert_eq!(hlen(queue), 2);
    assert_eq!(
        redis(&["GET", &idem]),
        held,
        "a retry must leave the held key in place"
    );

    redis_api::release_idempotency(queue, unique)
        .await
        .expect("release");
    assert_eq!(redis(&["GET", &idem]), "");

    let again_id = new_message_id();
    let again = redis_api::enqueue_job(job(queue, &again_id, 0, Some(unique)), None)
        .await
        .expect("reenqueue");
    assert_eq!(again.as_deref(), Some(again_id.as_str()));
    assert_eq!(hlen(queue), 3);
    assert!(!redis(&["GET", &idem]).is_empty());
    del_queue(queue);
}

async fn a_delayed_unique_task_is_placed_when_it_is_scheduled() {
    let queue = "uniqd";
    let unique = "recalc:proj-42";
    del_queue(queue);
    let idem = redis_api::idempotency_redis_key(queue, unique);
    let message_id = new_message_id();
    let queued = redis_api::enqueue_job(
        job(queue, &message_id, 0, Some(unique)),
        Some(Duration::from_secs(30)),
    )
    .await
    .expect("enqueue");
    assert_eq!(queued.as_deref(), Some(message_id.as_str()));
    assert_eq!(redis(&["ZCARD", &format!("{queue}:scheduled")]), "1");
    assert_eq!(redis(&["LLEN", &format!("{queue}:active")]), "0");
    let ttl: i64 = redis(&["TTL", &idem]).parse().expect("ttl");
    assert!(ttl > 0, "{ttl}");

    let duplicate = redis_api::enqueue_job(
        job(queue, &new_message_id(), 0, Some(unique)),
        Some(Duration::from_secs(30)),
    )
    .await
    .expect("duplicate");
    assert_eq!(duplicate, None);
    assert_eq!(redis(&["ZCARD", &format!("{queue}:scheduled")]), "1");
    assert_eq!(hlen(queue), 1);
    del_queue(queue);
}
