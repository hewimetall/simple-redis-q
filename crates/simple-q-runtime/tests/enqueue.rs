use std::process::Command;
use std::time::Duration;

use simple_q_runtime::MESSAGE_VERSION;
use simple_q_runtime::cleanup::{delete_worker_metadata, sweep_queue};
use simple_q_runtime::config::Config;
use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::redis_api;
use simple_q_runtime::redis_api::ConnectionManager;

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
    // Arrange
    // Act
    // Assert

    assert_eq!(
        redis_api::idempotency_redis_key("default", "recalc:42"),
        "default:idempotency:recalc:42"
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
    // Arrange

    let url = "redis://127.0.0.1:6379/15";
    let Ok(mut conn) = redis_api::connect(url).await else {
        return;
    };
    let settings = Config::defaults();
    a_delayed_message_stays_on_its_own_queue(&mut conn, &settings).await;
    sweep_removes_worker_metadata_absent_from_the_workers_set(&mut conn).await;
    a_unique_key_is_held_until_a_terminal_delete(&mut conn, &settings).await;
    a_delayed_unique_task_is_placed_when_it_is_scheduled(&mut conn, &settings).await; // Act

    a_retry_waiting_past_the_unique_ttl_still_holds_the_key(&mut conn).await; // Assert
}

async fn a_delayed_message_stays_on_its_own_queue(conn: &mut ConnectionManager, settings: &Config) {
    for key in [
        "qtest:scheduled",
        "qtest:data",
        "otherq:scheduled",
        "otherq:data",
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
    redis_api::enqueue_job(conn, settings, job, Some(Duration::from_secs(30)))
        .await
        .expect("enqueue")
        .expect("queued");
    assert_eq!(redis(&["ZCARD", "qtest:scheduled"]), "1");
    assert_eq!(redis(&["ZCARD", "otherq:scheduled"]), "0");
    let stored = redis(&["HVALS", "qtest:data"]);
    assert!(stored.contains(&message_id), "{stored}");
    assert_eq!(
        redis_api::incr_delivery(conn, &message_id, 0, 120)
            .await
            .expect("delivery"),
        1
    );
    let key = format!("delivery:{message_id}:0");
    assert_eq!(redis(&["GET", &key]), "1");
    let ttl = redis(&["TTL", &key]).parse::<i64>().expect("ttl");
    assert!((1..=120).contains(&ttl), "{ttl}");
    redis_api::release_delivery(conn, &message_id, 0)
        .await
        .expect("release");
    assert_eq!(redis(&["GET", &key]), "");
}

async fn sweep_removes_worker_metadata_absent_from_the_workers_set(conn: &mut ConnectionManager) {
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

    sweep_queue(conn, queue, 0, 0, 0, 10).await.expect("sweep");

    assert_eq!(
        redis(&["EXISTS", &gone_key]),
        "0",
        "absent member is removed"
    );
    assert_eq!(redis(&["EXISTS", &live_key]), "1", "present member is kept");
    assert_eq!(redis(&["ZSCORE", &workers, live_name]), "1");
    assert_eq!(redis(&["EXISTS", &format!("{queue}:data")]), "0");

    delete_worker_metadata(conn, queue, exit_name)
        .await
        .expect("delete metadata");
    assert_eq!(redis(&["EXISTS", &exit_key]), "0");

    for key in [&workers, &gone_key, &live_key, &exit_key] {
        let _ = redis(&["DEL", key]);
    }
}

async fn a_unique_key_is_held_until_a_terminal_delete(
    conn: &mut ConnectionManager,
    settings: &Config,
) {
    let queue = "uniq";
    let unique = "recalc:proj-42";
    del_queue(queue);
    let idem = redis_api::idempotency_redis_key(queue, unique);

    let first_id = new_message_id();
    let queued =
        redis_api::enqueue_job(conn, settings, job(queue, &first_id, 0, Some(unique)), None)
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

    let duplicate = redis_api::enqueue_job(
        conn,
        settings,
        job(queue, &new_message_id(), 0, Some(unique)),
        None,
    )
    .await
    .expect("duplicate");
    assert_eq!(duplicate, None);
    assert_eq!(
        hlen(queue),
        1,
        "a held key must not add a second queue entry"
    );
    assert_eq!(redis(&["GET", &idem]), held);

    let retry =
        redis_api::enqueue_job(conn, settings, job(queue, &first_id, 1, Some(unique)), None)
            .await
            .expect("retry");
    assert_eq!(retry.as_deref(), Some(first_id.as_str()));
    assert_eq!(hlen(queue), 2);
    assert_eq!(
        redis(&["GET", &idem]),
        held,
        "a retry must leave the held key in place"
    );

    redis_api::release_idempotency(conn, queue, unique)
        .await
        .expect("release");
    assert_eq!(redis(&["GET", &idem]), "");

    let again_id = new_message_id();
    let again =
        redis_api::enqueue_job(conn, settings, job(queue, &again_id, 0, Some(unique)), None)
            .await
            .expect("reenqueue");
    assert_eq!(again.as_deref(), Some(again_id.as_str()));
    assert_eq!(hlen(queue), 3);
    assert!(!redis(&["GET", &idem]).is_empty());
    del_queue(queue);
}

async fn a_delayed_unique_task_is_placed_when_it_is_scheduled(
    conn: &mut ConnectionManager,
    settings: &Config,
) {
    let queue = "uniqd";
    let unique = "recalc:proj-42";
    del_queue(queue);
    let idem = redis_api::idempotency_redis_key(queue, unique);
    let message_id = new_message_id();
    let queued = redis_api::enqueue_job(
        conn,
        settings,
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
        conn,
        settings,
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

async fn a_retry_waiting_past_the_unique_ttl_still_holds_the_key(conn: &mut ConnectionManager) {
    let settings = Config::defaults().with_unique_ttl(2).expect("ttl");
    let queue = "uniqwait";
    let unique = "recalc:42";
    del_queue(queue);
    let idem = redis_api::idempotency_redis_key(queue, unique);
    let message_id = new_message_id();
    let queued = redis_api::enqueue_job(
        conn,
        &settings,
        job(queue, &message_id, 0, Some(unique)),
        None,
    )
    .await
    .expect("enqueue");
    assert_eq!(queued.as_deref(), Some(message_id.as_str()));

    let delay = Duration::from_secs(5);
    let retry = redis_api::enqueue_job(
        conn,
        &settings,
        job(queue, &message_id, 1, Some(unique)),
        Some(delay),
    )
    .await
    .expect("retry");
    assert_eq!(retry.as_deref(), Some(message_id.as_str()));
    let ttl: i64 = redis(&["TTL", &idem]).parse().expect("ttl");
    assert!(
        ttl >= 6,
        "delay plus unique ttl should outlive the base ttl, got {ttl}"
    );

    let second = redis_api::enqueue_job(
        conn,
        &settings,
        job(queue, &new_message_id(), 0, Some(unique)),
        None,
    )
    .await
    .expect("second");
    assert_eq!(second, None);

    tokio::time::sleep(Duration::from_secs(3)).await;
    let ttl_after: i64 = redis(&["TTL", &idem]).parse().expect("ttl");
    assert!(ttl_after > 0, "{ttl_after}");
    let third = redis_api::enqueue_job(
        conn,
        &settings,
        job(queue, &new_message_id(), 0, Some(unique)),
        None,
    )
    .await
    .expect("third");
    assert_eq!(third, None);
    del_queue(queue);
}
