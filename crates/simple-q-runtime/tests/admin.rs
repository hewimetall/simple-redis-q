use std::process::Command;
use std::time::Duration;

use apalis::prelude::WorkerContext;
use simple_q_runtime::MESSAGE_VERSION;
use simple_q_runtime::admin::{self, ChildTrack, SupervisorRecord};
use simple_q_runtime::cleanup::sweep_queue;
use simple_q_runtime::config::Config;
use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::pages;
use simple_q_runtime::redis_api::{self, ConnectionManager};

fn redis(args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .args(["-n", "15"])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn redis_now() -> u64 {
    redis(&["TIME"])
        .lines()
        .next()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0)
}

fn token(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(0);
    format!("{prefix}{:x}{nanos:08x}", std::process::id())
}

struct Cleanup {
    keys: Vec<String>,
    members: Vec<String>,
}

impl Cleanup {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            members: Vec::new(),
        }
    }

    fn key(&mut self, key: impl Into<String>) {
        self.keys.push(key.into());
    }

    fn member(&mut self, member: impl Into<String>) {
        self.members.push(member.into());
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for key in &self.keys {
            let _ = redis(&["DEL", key]);
        }
        for member in &self.members {
            let _ = redis(&["ZREM", admin::SUPERVISORS_KEY, member]);
        }
    }
}

fn sample(queue: &str, message_id: &str, retries: u32) -> Job {
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
        unique_key: None,
    }
}

async fn connect() -> Option<ConnectionManager> {
    redis_api::connect("redis://127.0.0.1:6379/15").await.ok()
}

#[tokio::test]
async fn publish_shows_up_refreshes_ttl_and_expires() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let host = token("h");
    let pid = 41u32;
    let child_pid = 42u32;
    let member = admin::supervisor_member(&host, pid);
    let key = admin::supervisor_key(&member);
    let stale = token("stale");
    let kept = token("kept");
    let mut cleanup = Cleanup::new();
    cleanup.key(&key);
    cleanup.member(&member);
    cleanup.member(&stale);
    cleanup.member(&kept);
    let _ = redis(&["DEL", &key]);
    let _ = redis(&["ZREM", admin::SUPERVISORS_KEY, &member, &stale, &kept]);

    let mut child = ChildTrack::start(1).record();
    child.pid = child_pid;
    let record = SupervisorRecord {
        host: host.clone(),
        pid,
        started_at: 10,
        version: "1.0.0-rc1".to_owned(),
        queues: vec!["holdq".to_owned()],
        processes: 1,
        concurrency: 2,
        max_tasks: 500,
        child_soft: None,
        child_hard: Some(64),
        cgroup_current: None,
        cgroup_max: None,
        recycled: 0,
        drains: 0,
        kills: 0,
        restarts: 0,
        children: vec![child],
    };
    let now = redis_now();
    let _ = redis(&[
        "ZADD",
        admin::SUPERVISORS_KEY,
        "1",
        &stale,
        &now.to_string(),
        &kept,
    ]);
    admin::publish_supervisor(&mut conn, &settings, &record)
        .await
        .expect("publish");
    assert!(!redis(&["ZSCORE", admin::SUPERVISORS_KEY, &member]).is_empty());
    assert_eq!(redis(&["HGET", &key, "host"]), host);
    assert_eq!(redis(&["HGET", &key, "child_soft"]), "");
    assert_eq!(redis(&["HGET", &key, "child_hard"]), "64");
    assert!(
        redis(&["HGET", &key, &format!("child:{child_pid}")]).contains("running"),
        "child row"
    );
    assert!(redis(&["ZSCORE", admin::SUPERVISORS_KEY, &stale]).is_empty());
    assert!(!redis(&["ZSCORE", admin::SUPERVISORS_KEY, &kept]).is_empty());
    let ttl = redis(&["TTL", &key]).parse::<i64>().unwrap_or(-2);
    assert!(ttl > 10, "ttl {ttl}");

    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = redis(&["TTL", &key]).parse::<i64>().unwrap_or(-2);
    let refreshed = SupervisorRecord {
        children: Vec::new(),
        ..record
    };
    admin::publish_supervisor(&mut conn, &settings, &refreshed)
        .await
        .expect("refresh");
    let after = redis(&["TTL", &key]).parse::<i64>().unwrap_or(-2);
    assert!(after > before, "ttl {before} -> {after}");
    assert_eq!(redis(&["HGET", &key, "host"]), host);
    assert!(redis(&["HGET", &key, &format!("child:{child_pid}")]).is_empty());

    let _ = redis(&["EXPIRE", &key, "1"]);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert_eq!(redis(&["EXISTS", &key]), "0");
}

#[tokio::test]
async fn snapshot_tree_has_a_running_child_and_a_held_task() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let queue = token("q");
    let host = format!("h.{}.example", token(""));
    let sup_pid = 3u32;
    let child_pid = 7u32;
    let suffix = "abcd1234";
    let worker_name = format!("agent-worker.{queue}.{host}.{child_pid}.{suffix}");
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let worker = WorkerContext::new(&worker_name);
    let active = config.active_jobs_list();
    let data = config.job_data_hash();
    let workers = config.workers_set();
    let inflight = config.inflight_set_for(&worker);
    let metadata = config.worker_metadata_key(&worker);
    let member = admin::supervisor_member(&host, sup_pid);
    let sup_key = admin::supervisor_key(&member);
    let message_id = new_message_id();
    let delivery = redis_api::delivery_key(&message_id, 1);
    let mut cleanup = Cleanup::new();
    for key in [
        &active,
        &data,
        &workers,
        &inflight,
        &metadata,
        &sup_key,
        &delivery,
        &config.scheduled_jobs_set(),
        &config.done_jobs_set(),
        &config.dead_jobs_set(),
        &config.signal_list(),
    ] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }
    cleanup.member(&member);

    redis_api::enqueue_job(&mut conn, &settings, sample(&queue, &message_id, 1), None)
        .await
        .expect("enqueue")
        .expect("queued");
    let task_id = redis(&["HKEYS", &data]);
    assert!(!task_id.is_empty(), "stored id");
    assert_eq!(redis(&["ZCARD", &config.scheduled_jobs_set()]), "1");
    let meta = format!("{}:{task_id}", config.job_meta_hash());
    cleanup.key(&meta);
    let _ = redis(&["SADD", &inflight, &task_id]);
    let _ = redis(&["HSET", &meta, "locked_at", "1700000000"]);
    let _ = redis(&["SET", &delivery, "4"]);
    let now = redis_now();
    let _ = redis(&["ZADD", &workers, &now.to_string(), &worker_name]);
    admin::note_worker_start(
        &mut conn,
        &settings,
        &queue,
        &worker_name,
        sup_pid,
        Some(0),
        500,
    )
    .await
    .expect("start");
    let mut child = ChildTrack::start(0).record();
    child.pid = child_pid;
    child.started_at = now;
    admin::publish_supervisor(
        &mut conn,
        &settings,
        &SupervisorRecord {
            host: host.clone(),
            pid: sup_pid,
            started_at: now,
            version: "1.0.0-rc1".to_owned(),
            queues: vec![queue.clone()],
            processes: 1,
            concurrency: 1,
            max_tasks: 500,
            child_soft: None,
            child_hard: None,
            cgroup_current: None,
            cgroup_max: None,
            recycled: 0,
            drains: 0,
            kills: 0,
            restarts: 0,
            children: vec![child],
        },
    )
    .await
    .expect("publish");

    let snap = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("snapshot");
    assert_eq!(snap.supervisors.len(), 1);
    assert_eq!(snap.supervisors[0].host, host);
    assert_eq!(snap.supervisors[0].pid, sup_pid);
    assert!(snap.orphans.is_empty());
    assert_eq!(snap.supervisors[0].children.len(), 1);
    let child = &snap.supervisors[0].children[0];
    assert_eq!(child.pid, child_pid);
    assert_eq!(child.state, "running");
    assert_eq!(child.ppid, Some(sup_pid));
    assert_eq!(child.workers, vec![worker_name]);
    assert_eq!(child.tasks.len(), 1);
    let task = &child.tasks[0];
    assert_eq!(task.task_id, task_id);
    assert_eq!(task.actor_name, "recalc");
    assert_eq!(task.kwargs, serde_json::json!({"project": "42"}));
    assert_eq!(task.retries, 1);
    assert_eq!(task.deliveries, 4);
    assert_eq!(task.locked_at, Some(1_700_000_000));
    assert_eq!(task.message_id, message_id);
}

#[tokio::test]
async fn stale_metadata_is_a_killed_orphan_and_a_missing_hash_is_exited() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let queue = token("q");
    let host = token("orph");
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let stale_name = format!("agent-worker.{queue}.{host}.11.stale001");
    let gone_name = format!("agent-worker.{queue}.{host}.12.nometa01");
    let workers = config.workers_set();
    let stale_worker = WorkerContext::new(&stale_name);
    let gone_worker = WorkerContext::new(&gone_name);
    let stale_meta = config.worker_metadata_key(&stale_worker);
    let gone_meta = config.worker_metadata_key(&gone_worker);
    let mut cleanup = Cleanup::new();
    for key in [&workers, &stale_meta, &gone_meta] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }

    admin::note_worker_start(&mut conn, &settings, &queue, &stale_name, 99, None, 10)
        .await
        .expect("start");
    let now = redis_now();
    let stale_score = now.saturating_sub(settings.orphan_window_secs() + 30);
    let _ = redis(&[
        "ZADD",
        &workers,
        &stale_score.to_string(),
        &stale_name,
        &now.to_string(),
        &gone_name,
    ]);

    let snap = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("snapshot");
    assert!(snap.supervisors.is_empty());
    let stale = snap
        .orphans
        .iter()
        .find(|proc| proc.pid == 11)
        .expect("stale orphan");
    assert_eq!(stale.state, "killed");
    assert!(stale.reason.is_none());
    assert!(!stale.alive);
    let gone = snap
        .orphans
        .iter()
        .find(|proc| proc.pid == 12)
        .expect("missing hash");
    assert_eq!(gone.state, "exited");
    assert!(gone.reason.is_none());
}

#[tokio::test]
async fn queue_counts_follow_active_inflight_done_and_dead() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let queue = token("q");
    let host = token("stat");
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let name = format!("agent-worker.{queue}.{host}.21.stat0001");
    let worker = WorkerContext::new(&name);
    let active = config.active_jobs_list();
    let scheduled = config.scheduled_jobs_set();
    let done = config.done_jobs_set();
    let dead = config.dead_jobs_set();
    let workers = config.workers_set();
    let data = config.job_data_hash();
    let inflight = config.inflight_set_for(&worker);
    let metadata = config.worker_metadata_key(&worker);
    let signal = config.signal_list();
    let mut cleanup = Cleanup::new();
    for key in [
        &active, &scheduled, &done, &dead, &workers, &data, &inflight, &metadata, &signal,
    ] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }

    let before = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("before");
    assert_eq!(before.queues[0].active, 0);
    assert_eq!(before.queues[0].inflight, 0);
    assert_eq!(before.queues[0].done, 0);
    assert_eq!(before.queues[0].dead, 0);

    let message_id = new_message_id();
    redis_api::enqueue_job(&mut conn, &settings, sample(&queue, &message_id, 0), None)
        .await
        .expect("enqueue")
        .expect("queued");
    let queued = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("queued");
    assert_eq!(queued.queues[0].active, 0);
    assert_eq!(queued.queues[0].scheduled, 1);

    let task_id = redis(&["HKEYS", &data]);
    assert!(!task_id.is_empty(), "stored id");
    let meta = format!("{}:{task_id}", config.job_meta_hash());
    cleanup.key(&meta);
    let now = redis_now();
    let _ = redis(&["ZADD", &workers, &now.to_string(), &name]);
    let _ = redis(&["ZREM", &scheduled, &task_id]);
    let _ = redis(&["SADD", &inflight, &task_id]);
    let holding = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("holding");
    assert_eq!(holding.queues[0].active, 0);
    assert_eq!(holding.queues[0].inflight, 1);
    assert_eq!(holding.queues[0].workers, 1);
    assert_eq!(holding.queues[0].workers_alive, 1);

    let _ = redis(&["SREM", &inflight, &task_id]);
    let _ = redis(&["SADD", &done, &task_id]);
    let finished = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("done");
    assert_eq!(finished.queues[0].inflight, 0);
    assert_eq!(finished.queues[0].done, 1);

    let dead_id = format!("{task_id}-dead");
    let _ = redis(&["SADD", &dead, &dead_id]);
    let buried = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("dead");
    assert_eq!(buried.queues[0].dead, 1);

    let _ = redis(&["DEL", &done, &dead]);
    let _ = redis(&["ZADD", &done, "1", &task_id]);
    let _ = redis(&["ZADD", &dead, "1", &dead_id]);
    let sorted = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("sorted");
    assert_eq!(sorted.queues[0].done, 1);
    assert_eq!(sorted.queues[0].dead, 1);
}

#[tokio::test]
async fn started_at_stays_while_locked_at_moves() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let queue = token("q");
    let host = token("start");
    let sup_pid = 3u32;
    let child_pid = 7u32;
    let worker_name = format!("agent-worker.{queue}.{host}.{child_pid}.abcd1234");
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let worker = WorkerContext::new(&worker_name);
    let data = config.job_data_hash();
    let workers = config.workers_set();
    let inflight = config.inflight_set_for(&worker);
    let metadata = config.worker_metadata_key(&worker);
    let member = admin::supervisor_member(&host, sup_pid);
    let sup_key = admin::supervisor_key(&member);
    let message_id = new_message_id();
    let mut cleanup = Cleanup::new();
    for key in [
        &config.active_jobs_list(),
        &data,
        &workers,
        &inflight,
        &metadata,
        &sup_key,
        &config.scheduled_jobs_set(),
        &config.done_jobs_set(),
        &config.dead_jobs_set(),
        &config.signal_list(),
    ] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }
    cleanup.member(&member);

    redis_api::enqueue_job(&mut conn, &settings, sample(&queue, &message_id, 1), None)
        .await
        .expect("enqueue")
        .expect("queued");
    let task_id = redis(&["HKEYS", &data]);
    assert!(!task_id.is_empty(), "stored id");
    let meta = format!("{}:{task_id}", config.job_meta_hash());
    cleanup.key(&meta);

    let started = pages::note_delivery_start(&mut conn, &settings, &queue, &task_id)
        .await
        .expect("started_at");
    let first_lease = started.saturating_add(10).to_string();
    let _ = redis(&["HSET", &meta, "locked_at", &first_lease]);
    let _ = redis(&["SADD", &inflight, &task_id]);
    let now = redis_now();
    let _ = redis(&["ZADD", &workers, &now.to_string(), &worker_name]);
    admin::note_worker_start(
        &mut conn,
        &settings,
        &queue,
        &worker_name,
        sup_pid,
        Some(0),
        500,
    )
    .await
    .expect("start");
    let mut child = ChildTrack::start(0).record();
    child.pid = child_pid;
    child.started_at = now;
    admin::publish_supervisor(
        &mut conn,
        &settings,
        &SupervisorRecord {
            host: host.clone(),
            pid: sup_pid,
            started_at: now,
            version: "1.0.0-rc4".to_owned(),
            queues: vec![queue.clone()],
            processes: 1,
            concurrency: 1,
            max_tasks: 500,
            child_soft: None,
            child_hard: None,
            cgroup_current: None,
            cgroup_max: None,
            recycled: 0,
            drains: 0,
            kills: 0,
            restarts: 0,
            children: vec![child],
        },
    )
    .await
    .expect("publish");

    let first = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("snapshot");
    let task = &first.supervisors[0].children[0].tasks[0];
    assert_eq!(task.started_at, Some(started));
    assert_eq!(task.locked_at, Some(started + 10));

    let moved = started.saturating_add(40).to_string();
    let _ = redis(&["HSET", &meta, "locked_at", &moved]);
    let second = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("snapshot");
    let task = &second.supervisors[0].children[0].tasks[0];
    assert_eq!(task.started_at, Some(started));
    assert_eq!(task.locked_at, Some(started + 40));
    assert_eq!(redis(&["HGET", &meta, "started_at"]), started.to_string());

    let mut overwritten = started;
    let began = std::time::Instant::now();
    while overwritten == started && began.elapsed() < Duration::from_secs(3) {
        tokio::time::sleep(Duration::from_millis(200)).await;
        overwritten = pages::note_delivery_start(&mut conn, &settings, &queue, &task_id)
            .await
            .expect("next delivery");
    }
    assert!(overwritten > started, "next delivery overwrites started_at");
    assert_eq!(redis(&["HGET", &meta, "locked_at"]), moved);
    let third = admin::snapshot(&mut conn, &settings, &[queue.clone()], Some(&host))
        .await
        .expect("snapshot");
    assert_eq!(
        third.supervisors[0].children[0].tasks[0].started_at,
        Some(overwritten)
    );
    assert_eq!(
        third.supervisors[0].children[0].tasks[0].locked_at,
        Some(started + 40)
    );
}

#[tokio::test]
async fn dead_page_is_newest_first_and_the_cursor_is_exclusive() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let queue = token("q");
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let data = config.job_data_hash();
    let dead = config.dead_jobs_set();
    let index = pages::dead_index_key(&queue);
    let mut cleanup = Cleanup::new();
    for key in [
        &data,
        &dead,
        &index,
        &config.active_jobs_list(),
        &config.scheduled_jobs_set(),
        &config.signal_list(),
    ] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }

    let recorded = format!("{}-recorded", token("t"));
    let recorded_meta = format!("{}:{recorded}", config.job_meta_hash());
    cleanup.key(&recorded_meta);
    pages::record_dead(
        &mut conn,
        &settings,
        &queue,
        &recorded,
        "retries",
        Some("RuntimeError: boom"),
        2,
    )
    .await
    .expect("record");
    let dead_at = redis(&["HGET", &recorded_meta, "dead_at"]);
    assert_eq!(redis(&["HGET", &recorded_meta, "reason"]), "retries");
    assert_eq!(
        redis(&["HGET", &recorded_meta, "error"]),
        "RuntimeError: boom"
    );
    assert_eq!(redis(&["HGET", &recorded_meta, "deliveries"]), "2");
    assert_eq!(redis(&["ZSCORE", &index, &recorded]), dead_at);
    pages::record_dead(&mut conn, &settings, &queue, &recorded, "poison", None, 3)
        .await
        .expect("overwrite");
    assert_eq!(redis(&["HGET", &recorded_meta, "reason"]), "poison");
    assert_eq!(redis(&["HGET", &recorded_meta, "error"]), "");
    assert_eq!(redis(&["HGET", &recorded_meta, "deliveries"]), "3");

    let page_queue = token("q");
    let page_config = redis_api::queue_config(&page_queue, &settings).expect("queue");
    let page_data = page_config.job_data_hash();
    let page_index = pages::dead_index_key(&page_queue);
    let page_dead = page_config.dead_jobs_set();
    for key in [&page_data, &page_index, &page_dead] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }
    let rows = [
        (
            "task-d",
            "40",
            "recalc",
            "retries",
            "RuntimeError: boom",
            "1",
        ),
        (
            "task-c",
            "30",
            "recalc",
            "timeout",
            "TimeoutError: slow",
            "2",
        ),
        ("task-b", "30", "recalc", "poison", "", "4"),
        ("task-a", "20", "recalc", "invalid", "", "0"),
    ];
    for (task_id, score, actor, reason, error, deliveries) in rows {
        let meta = format!("{}:{task_id}", page_config.job_meta_hash());
        cleanup.key(&meta);
        let job = sample(
            &page_queue,
            &format!("msg-{task_id}"),
            deliveries.parse().unwrap_or(0),
        );
        let mut job = job;
        job.actor_name = actor.to_owned();
        let payload = serde_json::to_string(&job).unwrap();
        let _ = redis(&["HSET", &page_data, task_id, &payload]);
        let _ = redis(&["ZADD", &page_index, score, task_id]);
        let _ = redis(&[
            "HSET",
            &meta,
            "reason",
            reason,
            "dead_at",
            score,
            "deliveries",
            deliveries,
        ]);
        if !error.is_empty() {
            let _ = redis(&["HSET", &meta, "error", error]);
        }
    }
    assert_eq!(redis(&["SCARD", &page_dead]), "0");

    let limit_zero = pages::dead_page(&mut conn, &settings, &page_queue, 0, None).await;
    assert!(limit_zero.is_err());

    let first = pages::dead_page(&mut conn, &settings, &page_queue, 2, None)
        .await
        .expect("page");
    let ids: Vec<&str> = first
        .messages
        .iter()
        .map(|item| item.task_id.as_str())
        .collect();
    assert_eq!(ids, vec!["task-d", "task-c"]);
    assert_eq!(first.messages[0].reason, "retries");
    assert_eq!(
        first.messages[0].error.as_deref(),
        Some("RuntimeError: boom")
    );
    assert_eq!(first.messages[0].message_id, "msg-task-d");
    assert_eq!(
        first.messages[0].kwargs,
        serde_json::json!({"project": "42"})
    );
    assert_eq!(first.messages[0].deliveries, 1);
    assert_eq!(first.messages[0].dead_at, 40);
    assert_eq!(
        first.messages[1].error.as_deref(),
        Some("TimeoutError: slow")
    );
    assert!(first.messages[1].error.is_some());
    let cursor = first.cursor.clone().expect("next cursor");

    let second = pages::dead_page(&mut conn, &settings, &page_queue, 2, Some(&cursor))
        .await
        .expect("next");
    let ids: Vec<&str> = second
        .messages
        .iter()
        .map(|item| item.task_id.as_str())
        .collect();
    assert_eq!(ids, vec!["task-b", "task-a"]);
    assert!(second.messages[0].error.is_none());
    assert_eq!(second.messages[0].reason, "poison");
    assert_eq!(second.messages[0].deliveries, 4);
    assert!(second.cursor.is_none());

    let capped = pages::dead_page(&mut conn, &settings, &page_queue, 10_000, None)
        .await
        .expect("capped");
    assert_eq!(capped.messages.len(), 4);
    assert!(
        pages::dead_page(&mut conn, &settings, &page_queue, 2, Some("%%%"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn peek_pages_active_and_scheduled_from_the_data_hash() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let queue = token("q");
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let active = config.active_jobs_list();
    let scheduled = config.scheduled_jobs_set();
    let data = config.job_data_hash();
    let mut cleanup = Cleanup::new();
    for key in [&active, &scheduled, &data, &config.signal_list()] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }

    for task_id in ["act-1", "act-2", "act-3", "not-listed"] {
        let job = sample(&queue, &format!("msg-{task_id}"), 0);
        let payload = serde_json::to_string(&job).unwrap();
        let _ = redis(&["HSET", &data, task_id, &payload]);
    }
    let _ = redis(&["RPUSH", &active, "act-1", "act-2", "act-3"]);
    let waiting = pages::peek_page(&mut conn, &settings, &queue, "active", 2, None)
        .await
        .expect("active");
    let ids: Vec<&str> = waiting
        .tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    assert_eq!(ids, vec!["act-1", "act-2"]);
    assert!(waiting.tasks.iter().all(|task| task.run_at.is_none()));
    assert_eq!(waiting.tasks[0].message_id, "msg-act-1");
    assert_eq!(waiting.tasks[0].actor_name, "recalc");
    let cursor = waiting.cursor.clone().expect("active cursor");
    let rest = pages::peek_page(&mut conn, &settings, &queue, "active", 2, Some(&cursor))
        .await
        .expect("active rest");
    assert_eq!(rest.tasks.len(), 1);
    assert_eq!(rest.tasks[0].task_id, "act-3");
    assert!(rest.cursor.is_none());
    assert!(
        pages::peek_page(&mut conn, &settings, &queue, "active", 0, None)
            .await
            .is_err()
    );

    let _ = redis(&[
        "ZADD", &scheduled, "100", "sch-a", "200", "sch-c", "200", "sch-b",
    ]);
    for task_id in ["sch-a", "sch-b", "sch-c"] {
        let job = sample(&queue, &format!("msg-{task_id}"), 1);
        let payload = serde_json::to_string(&job).unwrap();
        let _ = redis(&["HSET", &data, task_id, &payload]);
    }
    let soon = pages::peek_page(&mut conn, &settings, &queue, "scheduled", 2, None)
        .await
        .expect("scheduled");
    assert_eq!(soon.tasks[0].task_id, "sch-a");
    assert_eq!(soon.tasks[0].run_at, Some(100));
    assert_eq!(soon.tasks[1].task_id, "sch-b");
    assert_eq!(soon.tasks[1].run_at, Some(200));
    let later = pages::peek_page(
        &mut conn,
        &settings,
        &queue,
        "scheduled",
        2,
        soon.cursor.as_deref(),
    )
    .await
    .expect("later");
    assert_eq!(later.tasks.len(), 1);
    assert_eq!(later.tasks[0].task_id, "sch-c");
    assert_eq!(later.tasks[0].run_at, Some(200));
    assert!(later.cursor.is_none());
    assert!(
        pages::peek_page(&mut conn, &settings, &queue, "done", 10, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sweep_removes_the_dead_index_member() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let queue = token("q");
    let settings = Config::defaults();
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let data = config.job_data_hash();
    let dead = config.dead_jobs_set();
    let index = pages::dead_index_key(&queue);
    let old_id = "old-dead";
    let fresh_id = "fresh-dead";
    let old_meta = format!("{}:{old_id}", config.job_meta_hash());
    let fresh_meta = format!("{}:{fresh_id}", config.job_meta_hash());
    let mut cleanup = Cleanup::new();
    for key in [&data, &dead, &index, &old_meta, &fresh_meta] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }
    let now = redis_now();
    let old = now.saturating_sub(100).to_string();
    let fresh = now.to_string();
    let _ = redis(&["HSET", &data, old_id, "old", fresh_id, "fresh"]);
    let _ = redis(&["HSET", &old_meta, "done_at", &old]);
    let _ = redis(&["HSET", &fresh_meta, "done_at", &fresh]);
    let _ = redis(&["SADD", &dead, old_id, fresh_id]);
    let _ = redis(&["ZADD", &index, &old, old_id, &fresh, fresh_id]);

    let stats = sweep_queue(&mut conn, &queue, now as i64, 10, 10, 10)
        .await
        .expect("sweep");
    assert_eq!(stats.dead, 1);
    assert_eq!(redis(&["ZSCORE", &index, old_id]), "");
    assert_eq!(redis(&["SISMEMBER", &dead, old_id]), "0");
    assert_eq!(redis(&["HGET", &data, old_id]), "");
    assert_eq!(redis(&["EXISTS", &old_meta]), "0");
    assert_eq!(redis(&["ZSCORE", &index, fresh_id]), fresh);
    assert_eq!(redis(&["SISMEMBER", &dead, fresh_id]), "1");
}

#[tokio::test]
async fn sweep_removes_a_member_from_a_dead_sorted_set() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let queue = token("q");
    let settings = Config::defaults();
    let config = redis_api::queue_config(&queue, &settings).expect("queue");
    let data = config.job_data_hash();
    let dead = config.dead_jobs_set();
    let index = pages::dead_index_key(&queue);
    let old_id = "old-zdead";
    let fresh_id = "fresh-zdead";
    let old_meta = format!("{}:{old_id}", config.job_meta_hash());
    let fresh_meta = format!("{}:{fresh_id}", config.job_meta_hash());
    let mut cleanup = Cleanup::new();
    for key in [&data, &dead, &index, &old_meta, &fresh_meta] {
        cleanup.key(key);
        let _ = redis(&["DEL", key]);
    }
    let now = redis_now();
    let old = now.saturating_sub(100).to_string();
    let fresh = now.to_string();
    let _ = redis(&["HSET", &data, old_id, "old", fresh_id, "fresh"]);
    let _ = redis(&["HSET", &old_meta, "done_at", &old]);
    let _ = redis(&["HSET", &fresh_meta, "done_at", &fresh]);
    let _ = redis(&["ZADD", &dead, &old, old_id, &fresh, fresh_id]);
    let _ = redis(&["ZADD", &index, &old, old_id, &fresh, fresh_id]);

    let stats = sweep_queue(&mut conn, &queue, now as i64, 10, 10, 10)
        .await
        .expect("sweep");
    assert_eq!(stats.dead, 1);
    assert_eq!(redis(&["ZSCORE", &index, old_id]), "");
    assert_eq!(redis(&["ZSCORE", &dead, old_id]), "");
    assert_eq!(redis(&["HGET", &data, old_id]), "");
    assert_eq!(redis(&["EXISTS", &old_meta]), "0");
    assert_eq!(redis(&["ZSCORE", &index, fresh_id]), fresh);
    assert_eq!(redis(&["ZSCORE", &dead, fresh_id]), fresh);
}

#[tokio::test]
async fn snapshot_includes_tasks_from_every_supervisor_queue() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let settings = Config::defaults();
    let quota = token("q");
    let default_queue = token("d");
    let other = token("o");
    let host = token("tree");
    let sup_pid = 3u32;
    let child_pid = 7u32;
    let quota_worker = format!("agent-worker.{quota}.{host}.{child_pid}.abcd1234");
    let default_worker = format!("agent-worker.{default_queue}.{host}.{child_pid}.abcd1234");
    let other_worker = format!("agent-worker.{other}.{host}.{child_pid}.abcd1234");
    let member = admin::supervisor_member(&host, sup_pid);
    let sup_key = admin::supervisor_key(&member);
    let mut cleanup = Cleanup::new();
    cleanup.key(&sup_key);
    cleanup.member(&member);
    let _ = redis(&["DEL", &sup_key]);

    for (queue, worker_name, task_id) in [
        (&quota, &quota_worker, "quota-task"),
        (&default_queue, &default_worker, "default-task"),
        (&other, &other_worker, "other-task"),
    ] {
        let config = redis_api::queue_config(queue, &settings).expect("queue");
        let worker = WorkerContext::new(worker_name);
        let workers = config.workers_set();
        let inflight = config.inflight_set_for(&worker);
        let metadata = config.worker_metadata_key(&worker);
        let data = config.job_data_hash();
        for key in [
            &workers,
            &inflight,
            &metadata,
            &data,
            &config.active_jobs_list(),
            &config.scheduled_jobs_set(),
            &config.dead_jobs_set(),
            &config.done_jobs_set(),
            &config.signal_list(),
        ] {
            cleanup.key(key);
            let _ = redis(&["DEL", key]);
        }
        let now = redis_now();
        let _ = redis(&["ZADD", &workers, &now.to_string(), worker_name]);
        let _ = redis(&["SADD", &inflight, task_id]);
        admin::note_worker_start(
            &mut conn,
            &settings,
            queue,
            worker_name,
            sup_pid,
            Some(0),
            500,
        )
        .await
        .expect("worker");
    }

    let now = redis_now();
    let mut child = ChildTrack::start(0).record();
    child.pid = child_pid;
    child.started_at = now;
    admin::publish_supervisor(
        &mut conn,
        &settings,
        &SupervisorRecord {
            host: host.clone(),
            pid: sup_pid,
            started_at: now,
            version: "1.0.0-rc4".to_owned(),
            queues: vec![quota.clone(), default_queue.clone()],
            processes: 1,
            concurrency: 1,
            max_tasks: 500,
            child_soft: None,
            child_hard: None,
            cgroup_current: None,
            cgroup_max: None,
            recycled: 0,
            drains: 0,
            kills: 0,
            restarts: 0,
            children: vec![child],
        },
    )
    .await
    .expect("publish");

    let snap = admin::snapshot(&mut conn, &settings, &[quota.clone()], Some(&host))
        .await
        .expect("snapshot");
    assert_eq!(snap.queues.len(), 1);
    assert_eq!(snap.queues[0].name, quota);
    assert_eq!(snap.queues[0].inflight, 1);
    let child = &snap.supervisors[0].children[0];
    let tasks: Vec<&str> = child
        .tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    assert_eq!(tasks, vec!["default-task", "quota-task"]);
    assert!(child.workers.contains(&quota_worker));
    assert!(child.workers.contains(&default_worker));
    assert!(!child.workers.iter().any(|name| name == &other_worker));
}
