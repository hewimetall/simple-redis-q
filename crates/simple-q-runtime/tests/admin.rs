use std::process::Command;
use std::time::Duration;

use apalis::prelude::WorkerContext;
use simple_q_runtime::MESSAGE_VERSION;
use simple_q_runtime::admin::{self, ChildTrack, SupervisorRecord};
use simple_q_runtime::config::Config;
use simple_q_runtime::job::{Job, new_message_id};
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
}
