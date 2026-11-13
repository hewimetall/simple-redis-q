//! Idle fetch uses Apalis `poll_with_interval`. Heartbeat stays the orphan clock.

use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use apalis::prelude::{Monitor, WorkerBuilder, WorkerContext};
use simple_q_runtime::MESSAGE_VERSION;
use simple_q_runtime::config::Config;
use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::redis_api::{self, ConnectionManager};

const REDIS_URL: &str = "redis://127.0.0.1:6379/15";

fn redis(args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .args(["-n", "15"])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn job(queue: &str) -> Job {
    Job {
        version: MESSAGE_VERSION,
        message_id: new_message_id(),
        actor_name: "recalc".to_owned(),
        queue: queue.to_owned(),
        kwargs: serde_json::json!({"n": 1}),
        headers: Default::default(),
        retries: 0,
        max_retries: 0,
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
        let _ = redis(&["DEL", &format!("{queue}:{suffix}")]);
    }
    for pattern in [
        format!("{queue}:meta:*"),
        format!("{queue}:inflight:*"),
        format!("{queue}:workers:metadata:*"),
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

fn settings(heartbeat_secs: u64, missed: u64, poll_ms: u64) -> Config {
    let mut settings = Config::defaults();
    settings.heartbeat_secs = heartbeat_secs;
    settings.missed_heartbeats = missed;
    settings.poll_interval_ms = poll_ms;
    settings
}

async fn connect() -> ConnectionManager {
    redis_api::connect(REDIS_URL).await.expect("redis on db 15")
}

fn workers(queue: &str) -> i64 {
    redis(&["ZCARD", &format!("{queue}:workers")])
        .parse()
        .unwrap_or(0)
}

async fn wait_for_workers(queue: &str, want: i64) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if workers(queue) >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("queue {queue} has {} workers", workers(queue));
}

fn info_number(section: &str, key: &str) -> f64 {
    let text = redis(&["INFO", section]);
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(&format!("{key}:")) else {
            continue;
        };
        if let Some(calls) = rest.split(',').find_map(|part| part.strip_prefix("calls=")) {
            return calls.parse().unwrap_or(0.0);
        }
        return rest.parse().unwrap_or(0.0);
    }
    0.0
}

fn script_calls() -> f64 {
    info_number("commandstats", "cmdstat_eval") + info_number("commandstats", "cmdstat_evalsha")
}

fn redis_cpu() -> f64 {
    info_number("cpu", "used_cpu_user") + info_number("cpu", "used_cpu_sys")
}

struct KillOnDrop(Option<Child>);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn_harness(test: &str, envs: &[(&str, &str)]) -> Child {
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .arg(test)
        .arg("--exact")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in envs {
        command.env(key, value);
    }
    command.spawn().expect("spawn test child")
}

#[tokio::test]
async fn poll_child_holds_one_task() {
    if std::env::var("SIMPLE_Q_POLL_HOLD").ok().as_deref() != Some("1") {
        return;
    }
    let queue = std::env::var("SIMPLE_Q_POLL_QUEUE").expect("queue");
    let held = std::env::var("SIMPLE_Q_POLL_HELD").expect("held path");
    let settings = Config::from_env().expect("config");
    let conn = connect().await;
    let storage = redis_api::worker_storage(conn, &queue, &settings).expect("storage");
    let worker = WorkerBuilder::new(format!("hold{}", std::process::id()))
        .backend(storage)
        .build(move |job: Job| {
            let held = held.clone();
            async move {
                std::fs::write(&held, job.message_id).expect("held file");
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok::<(), std::io::Error>(())
            }
        });
    worker.run().await.expect("holder");
}

#[tokio::test]
async fn poll_child_idles() {
    if std::env::var("SIMPLE_Q_POLL_IDLE").ok().as_deref() != Some("1") {
        return;
    }
    let queues = std::env::var("SIMPLE_Q_POLL_QUEUES").expect("queues");
    let settings = Config::from_env().expect("config");
    let conn = connect().await;
    let mut monitor = Monitor::new();
    for queue in queues.split(',') {
        let queue = queue.to_owned();
        let storage = redis_api::worker_storage(conn.clone(), &queue, &settings).expect("storage");
        let name = format!("idle{queue}{}", std::process::id());
        monitor = monitor.register(move |_| {
            WorkerBuilder::new(name.clone())
                .backend(storage.clone())
                .build(|_job: Job| async { Ok::<(), std::io::Error>(()) })
        });
    }
    monitor.run().await.expect("idle workers");
}

#[tokio::test]
async fn one_task_after_idle_arrives_before_the_heartbeat() {
    let queue = format!("pollidle{}", std::process::id());
    del_queue(&queue);
    let settings = settings(15, 4, 500);
    let conn = connect().await;
    let storage = redis_api::worker_storage(conn.clone(), &queue, &settings).expect("storage");
    let seen = Arc::new(Mutex::new(None));
    let slot = seen.clone();
    let worker = WorkerBuilder::new(format!("idle{queue}"))
        .backend(storage)
        .build(move |job: Job, ctx: WorkerContext| {
            let slot = slot.clone();
            async move {
                *slot.lock().expect("seen") = Some(job.message_id);
                ctx.stop().expect("stop");
                Ok::<(), std::io::Error>(())
            }
        });
    let running = tokio::spawn(async move { worker.run().await });
    wait_for_workers(&queue, 1).await;
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut conn = conn;
    let job = job(&queue);
    let message_id = job.message_id.clone();
    let started = Instant::now();
    redis_api::enqueue_job(&mut conn, &settings, job, None)
        .await
        .expect("enqueue")
        .expect("queued");

    let deadline = Duration::from_millis(1_500);
    while started.elapsed() < deadline && seen.lock().expect("seen").is_none() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = started.elapsed();
    println!("POLL_MEASURE pickup_ms={}", elapsed.as_millis());
    assert_eq!(
        seen.lock().expect("seen").as_deref(),
        Some(message_id.as_str())
    );
    assert!(
        elapsed < deadline,
        "pickup took {elapsed:?}; the heartbeat is 15s"
    );
    running.abort();
    del_queue(&queue);
}

#[tokio::test]
async fn a_batch_after_idle_is_drained_without_waiting_out_the_heartbeat() {
    let queue = format!("pollbatch{}", std::process::id());
    del_queue(&queue);
    let settings = settings(15, 4, 500);
    let conn = connect().await;
    let storage = redis_api::worker_storage(conn.clone(), &queue, &settings).expect("storage");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let slot = seen.clone();
    let worker = WorkerBuilder::new(format!("batch{queue}"))
        .backend(storage)
        .build(move |job: Job, ctx: WorkerContext| {
            let slot = slot.clone();
            async move {
                let mut seen = slot.lock().expect("seen");
                seen.push(job.message_id);
                if seen.len() == 20 {
                    ctx.stop().expect("stop");
                }
                Ok::<(), std::io::Error>(())
            }
        });
    let running = tokio::spawn(async move { worker.run().await });
    wait_for_workers(&queue, 1).await;
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut conn = conn;
    let started = Instant::now();
    for _ in 0..20 {
        redis_api::enqueue_job(&mut conn, &settings, job(&queue), None)
            .await
            .expect("enqueue")
            .expect("queued");
    }
    let deadline = Duration::from_secs(4);
    while started.elapsed() < deadline && seen.lock().expect("seen").len() < 20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = started.elapsed();
    let count = seen.lock().expect("seen").len();
    println!(
        "POLL_MEASURE batch_ms={} count={count}",
        elapsed.as_millis()
    );
    assert_eq!(count, 20, "batch still short after {elapsed:?}");
    assert!(elapsed < deadline, "batch took {elapsed:?}");
    running.abort();
    del_queue(&queue);
}

#[tokio::test]
async fn sigkill_waits_for_the_orphan_window_then_the_poll() {
    let queue = format!("pollkill{}", std::process::id());
    del_queue(&queue);
    let held = std::env::temp_dir().join(format!("{queue}.held"));
    let _ = std::fs::remove_file(&held);
    let settings = settings(2, 2, 200);
    let mut holder = KillOnDrop(Some(spawn_harness(
        "poll_child_holds_one_task",
        &[
            ("SIMPLE_Q_POLL_HOLD", "1"),
            ("SIMPLE_Q_POLL_QUEUE", &queue),
            ("SIMPLE_Q_POLL_HELD", held.to_str().expect("path")),
            ("SIMPLE_Q_HEARTBEAT_SECS", "2"),
            ("SIMPLE_Q_MISSED_HEARTBEATS", "2"),
            ("SIMPLE_Q_POLL_INTERVAL_MS", "200"),
        ],
    )));
    wait_for_workers(&queue, 1).await;
    let mut conn = connect().await;
    let sent = job(&queue);
    let message_id = sent.message_id.clone();
    redis_api::enqueue_job(&mut conn, &settings, sent, None)
        .await
        .expect("enqueue")
        .expect("queued");

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) && !held.exists() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(held.exists(), "holder never took the task");
    assert_eq!(std::fs::read_to_string(&held).expect("held"), message_id);

    let killed_at = Instant::now();
    if let Some(child) = holder.0.as_mut() {
        child.kill().expect("sigkill");
        let _ = child.wait();
    }
    holder.0 = None;

    let storage = redis_api::worker_storage(conn.clone(), &queue, &settings).expect("storage");
    let seen = Arc::new(Mutex::new(None));
    let slot = seen.clone();
    let worker = WorkerBuilder::new(format!("next{queue}"))
        .backend(storage)
        .build(move |job: Job, ctx: WorkerContext| {
            let slot = slot.clone();
            async move {
                *slot.lock().expect("seen") = Some(job.message_id);
                ctx.stop().expect("stop");
                Ok::<(), std::io::Error>(())
            }
        });
    let running = tokio::spawn(async move { worker.run().await });
    let deadline = Duration::from_secs(12);
    while killed_at.elapsed() < deadline && seen.lock().expect("seen").is_none() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = killed_at.elapsed();
    println!("POLL_MEASURE sigkill_redelivery_ms={}", elapsed.as_millis());
    assert_eq!(
        seen.lock().expect("seen").as_deref(),
        Some(message_id.as_str())
    );
    assert!(
        elapsed >= Duration::from_millis(1_000),
        "redelivery in {elapsed:?} did not wait for the orphan window"
    );
    assert!(elapsed < deadline, "redelivery took {elapsed:?}");
    running.abort();
    let _ = std::fs::remove_file(&held);
    del_queue(&queue);
}

#[tokio::test]
async fn idle_polls_are_measured_across_processes_and_queues() {
    let pid = std::process::id();
    let queues = [
        format!("pollpa{pid}"),
        format!("pollpb{pid}"),
        format!("pollpc{pid}"),
        format!("pollpd{pid}"),
    ];
    for queue in &queues {
        del_queue(queue);
    }
    let left = format!("{},{}", queues[0], queues[1]);
    let right = format!("{},{}", queues[2], queues[3]);
    let common = [
        ("SIMPLE_Q_POLL_IDLE", "1"),
        ("SIMPLE_Q_HEARTBEAT_SECS", "15"),
        ("SIMPLE_Q_MISSED_HEARTBEATS", "4"),
        ("SIMPLE_Q_POLL_INTERVAL_MS", "500"),
    ];
    let mut left_env = common.to_vec();
    left_env.push(("SIMPLE_Q_POLL_QUEUES", left.as_str()));
    let mut right_env = common.to_vec();
    right_env.push(("SIMPLE_Q_POLL_QUEUES", right.as_str()));
    let first = KillOnDrop(Some(spawn_harness("poll_child_idles", &left_env)));
    let second = KillOnDrop(Some(spawn_harness("poll_child_idles", &right_env)));
    for queue in &queues {
        wait_for_workers(queue, 1).await;
    }
    tokio::time::sleep(Duration::from_millis(700)).await;

    let before_calls = script_calls();
    let before_cpu = redis_cpu();
    let window = Duration::from_secs(5);
    tokio::time::sleep(window).await;
    let calls = script_calls() - before_calls;
    let cpu = redis_cpu() - before_cpu;
    let per_sec = calls / window.as_secs_f64();
    println!(
        "POLL_MEASURE processes=2 queues=4 interval_ms=500 idle_secs=5 script_calls={calls:.0} scripts_per_sec={per_sec:.2} redis_cpu_seconds={cpu:.4}"
    );
    assert!(
        per_sec >= 4.0,
        "idle fetch rate {per_sec:.2}/s is still a heartbeat, not a 500ms poll"
    );
    drop(first);
    drop(second);
    for queue in &queues {
        del_queue(queue);
    }
}
