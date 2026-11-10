use std::process::Command;
use std::time::Duration;

use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::redis_api;
use simple_q_runtime::MESSAGE_VERSION;

fn redis(args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .args(["-n", "15"])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[tokio::test]
async fn a_delayed_message_stays_on_its_own_queue() {
    let url = "redis://127.0.0.1:6379/15".to_owned();
    if redis_api::install(url).await.is_err() {
        return;
    }
    for key in ["qtest:scheduled", "qtest:data", "quota:scheduled", "quota:data"] {
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
    };
    redis_api::enqueue_job(job, Some(Duration::from_secs(30)))
        .await
        .expect("enqueue");
    assert_eq!(redis(&["ZCARD", "qtest:scheduled"]), "1");
    assert_eq!(redis(&["ZCARD", "quota:scheduled"]), "0");
    let stored = redis(&["HVALS", "qtest:data"]);
    assert!(stored.contains(&message_id), "{stored}");
    assert_eq!(
        redis_api::incr_delivery(&message_id, 0).await.expect("delivery"),
        1
    );
    assert_eq!(redis(&["GET", &format!("delivery:{message_id}:0")]), "1");
}
