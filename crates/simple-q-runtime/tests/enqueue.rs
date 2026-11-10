use std::process::Command;
use std::time::Duration;

use simple_q_runtime::MESSAGE_VERSION;
use simple_q_runtime::cleanup::{delete_worker_metadata, sweep_queue};
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

#[tokio::test]
async fn a_delayed_message_stays_on_its_own_queue() {
    let url = "redis://127.0.0.1:6379/15".to_owned();
    if redis_api::install(url).await.is_err() {
        return;
    }
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
    };
    redis_api::enqueue_job(job, Some(Duration::from_secs(30)))
        .await
        .expect("enqueue");
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

#[tokio::test]
async fn sweep_removes_worker_metadata_absent_from_the_workers_set() {
    let url = "redis://127.0.0.1:6379/15".to_owned();
    if redis_api::install(url).await.is_err() && !redis_api::is_configured() {
        return;
    }
    let queue = "wmeta745b";
    let workers = format!("{queue}:workers");
    let gone_name = "agent-worker.wmeta745b.host.1.gone0001";
    let live_name = "agent-worker.wmeta745b.host.1.live0001";
    let gone_key = format!("{queue}:workers:metadata:{gone_name}");
    let live_key = format!("{queue}:workers:metadata:{live_name}");
    let exit_name = "agent-worker.wmeta745b.host.1.exit0001";
    let exit_key = format!("{queue}:workers:metadata:{exit_name}");
    for key in [&workers, &gone_key, &live_key, &exit_key] {
        let _ = redis(&["DEL", key]);
    }
    let _ = redis(&[
        "HSET",
        &gone_key,
        "backend_name",
        "redis",
        "service",
        "gone",
    ]);
    let _ = redis(&[
        "HSET",
        &live_key,
        "backend_name",
        "redis",
        "service",
        "live",
    ]);
    let _ = redis(&["ZADD", &workers, "1", live_name]);
    let _ = redis(&[
        "HSET",
        &exit_key,
        "backend_name",
        "redis",
        "service",
        "exit",
    ]);

    sweep_queue(queue, 0, 0, 0, 10).await.expect("sweep");

    assert_eq!(
        redis(&["EXISTS", &gone_key]),
        "0",
        "absent member is removed"
    );
    assert_eq!(redis(&["EXISTS", &live_key]), "1", "present member is kept");
    assert_eq!(redis(&["ZSCORE", &workers, live_name]), "1");
    assert_eq!(redis(&["EXISTS", &format!("{queue}:data")]), "0");

    delete_worker_metadata(queue, exit_name)
        .await
        .expect("delete metadata");
    assert_eq!(redis(&["EXISTS", &exit_key]), "0");

    for key in [&workers, &gone_key, &live_key, &exit_key] {
        let _ = redis(&["DEL", key]);
    }
}
