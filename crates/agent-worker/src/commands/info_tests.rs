use super::*;
use simple_q_runtime::admin::{HeldTask, ProcessInfo, QueueStats, Snapshot, SupervisorInfo};

#[test]
fn format_bytes_uses_binary_units() {
    // Arrange
    // Act
    // Assert

    assert_eq!(format_bytes(4 * 1024 * 1024 * 1024), "4G");
    assert_eq!(format_bytes(812 * 1024 * 1024), "812M");
    assert_eq!(format_bytes(1500 * 1024 * 1024), "1500M");
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1G");
    assert_eq!(format_bytes(640 * 1024 * 1024), "640M");
    assert_eq!(
        format_bytes((2.1_f64 * 1024.0 * 1024.0 * 1024.0).round() as u64),
        "2.1G"
    );
    assert_eq!(
        format_bytes((1.02 * 1024f64.powi(3)).round() as u64),
        "1.02G"
    );
    assert_eq!(
        format_bytes((1.51 * 1024f64.powi(3)).round() as u64),
        "1.51G"
    );
    assert_eq!(format_bytes(351_112 * 1024), "342.9M");
    assert_eq!(format_bytes(9_999 * 1024 * 1024), "9999M");
    assert_eq!(format_bytes(10_000 * 1024 * 1024), "9.77G");
    assert_eq!(format_uptime(1_000, 1_045), "45s");
    assert_eq!(format_uptime(1_000, 1_000 + 38 * 60), "38m");
    assert_eq!(format_uptime(1_000, 1_000 + 2 * 3_600 + 5 * 60), "2h5m");
    assert_eq!(display_version("1.0.0-rc7"), "1.0.0rc7");
    assert_eq!(display_version("1.1.0"), "1.1.0");
}

#[test]
fn one_supervisor_block_lists_children_tasks_and_queue_depth() {
    // Arrange

    let started_at = 1_700_000_000_u64;
    let taken_at = started_at + 38 * 60;
    let running = task(
        31,
        serde_json::json!({"project_id": "3f1c0b2a-9c7e-4d11-8a55-6b0e1f2a3c4d"}),
        taken_at,
    );
    let held = task(23, serde_json::json!({"project_id": "held"}), taken_at);
    let snapshot = Snapshot {
        taken_at,
        queues: vec![QueueStats {
            name: "quota".to_owned(),
            active: 12,
            scheduled: 1,
            inflight: 4,
            done: 9873,
            dead: 0,
            workers: 3,
            workers_alive: 3,
        }],
        supervisors: vec![SupervisorInfo {
            host: "worker-quota-6f8b9c7d4-x2k9p".to_owned(),
            pid: 1,
            started_at,
            last_seen: taken_at,
            version: "1.1.0".to_owned(),
            queues: vec!["quota".to_owned()],
            processes: 3,
            concurrency: 1,
            max_tasks: 500,
            child_soft: Some(1024 * 1024 * 1024),
            child_hard: Some(1500 * 1024 * 1024),
            cgroup_current: Some((2.1_f64 * 1024.0 * 1024.0 * 1024.0).round() as u64),
            cgroup_max: Some(4 * 1024 * 1024 * 1024),
            recycled: 14,
            drains: 2,
            kills: 1,
            restarts: 0,
            children: vec![
                child(
                    0,
                    31,
                    "running",
                    None,
                    Some(812 * 1024 * 1024),
                    412,
                    4,
                    false,
                    vec![running],
                    taken_at,
                ),
                child(
                    1,
                    57,
                    "draining",
                    Some("max_tasks"),
                    Some(640 * 1024 * 1024),
                    500,
                    9,
                    false,
                    vec![],
                    taken_at,
                ),
                child(
                    2,
                    44,
                    "draining",
                    Some("memory_soft"),
                    Some((1.02 * 1024f64.powi(3)).round() as u64),
                    212,
                    3,
                    false,
                    vec![],
                    taken_at,
                ),
                child(
                    0,
                    23,
                    "killed",
                    Some("memory_hard"),
                    Some((1.51 * 1024f64.powi(3)).round() as u64),
                    98,
                    41,
                    true,
                    vec![held],
                    taken_at,
                ),
                child(
                    1,
                    12,
                    "exited",
                    Some("max_tasks"),
                    None,
                    500,
                    52,
                    true,
                    vec![],
                    taken_at,
                ),
            ],
        }],
        orphans: vec![],
    };
    let text = render_text(&snapshot, false);
    // Act

    let expected = "\
supervisor  worker-quota-6f8b9c7d4-x2k9p  pid 1  up 38m  simple-q 1.1.0
queues      quota  processes 3/3  concurrency 1  max_tasks 500
memory      cgroup 2.1G / 4G  child soft 1G / hard 1500M
counters    recycled 14  soft 2  hard 1  restarts 0
SLOT  PID  STATE     REASON       RSS    TASKS    HB
0     31   running                812M   412/500  4s
1     57   draining  max_tasks    640M   500/500  9s
2     44   draining  memory_soft  1.02G  212/500  3s
0     23   killed    memory_hard  1.51G  98/500   41s   held 1, waiting for requeue
1     12   exited    max_tasks    -      500/500  52s
PID  QUEUE  ACTOR                        KWARGS                  TRY  DLV  RUN
31   quota  recalc                       project_id=3f1c0b2a…    0/1  1    212s/1800s
23   quota  recalc                       project_id=held         0/1  1    212s/1800s
QUEUE  ACTIVE  SCHEDULED  INFLIGHT  DONE  DEAD
quota  12      1          4         9873  0";
    // Assert

    assert_eq!(text, expected);
}

#[test]
fn queue_fields_accept_json_and_commas() {
    // Arrange
    // Act
    // Assert

    assert_eq!(parse_queue_field("quota"), vec!["quota".to_owned()]);
    assert_eq!(
        parse_queue_field("quota,default"),
        vec!["quota".to_owned(), "default".to_owned()]
    );
    assert_eq!(
        parse_queue_field("[\"quota\",\"default\"]"),
        vec!["quota".to_owned(), "default".to_owned()]
    );
}

#[allow(clippy::too_many_arguments)]
fn child(
    slot: u32,
    pid: u32,
    state: &str,
    reason: Option<&str>,
    rss: Option<u64>,
    tasks_done: u64,
    last_seen_ago: u64,
    ended: bool,
    tasks: Vec<HeldTask>,
    taken_at: u64,
) -> ProcessInfo {
    ProcessInfo {
        host: "worker-quota-6f8b9c7d4-x2k9p".to_owned(),
        pid,
        ppid: Some(1),
        slot: Some(slot),
        state: state.to_owned(),
        reason: reason.map(str::to_owned),
        exit: None,
        rss,
        started_at: taken_at,
        ended_at: ended.then_some(taken_at),
        tasks_done: Some(tasks_done),
        max_tasks: Some(500),
        workers: Vec::new(),
        last_seen: Some(taken_at - last_seen_ago),
        alive: !ended,
        tasks,
    }
}

fn task(_pid: u32, kwargs: Value, taken_at: u64) -> HeldTask {
    HeldTask {
        queue: "quota".to_owned(),
        task_id: "task-1".to_owned(),
        message_id: "m-1".to_owned(),
        actor_name: "recalc".to_owned(),
        kwargs,
        retries: 0,
        max_retries: 1,
        time_limit_ms: 1_800_000,
        locked_at: Some(taken_at - 212),
        started_at: Some(taken_at - 212),
        deliveries: 1,
    }
}

#[test]
fn member_is_host_requires_the_pid_suffix() {
    // Arrange
    // Act
    // Assert

    assert!(member_is_host("web.staging.12", "web.staging", None));
    assert!(!member_is_host("web.staging.12", "web", None));
    assert!(member_is_host("web.staging.12", "web", Some("web")));
    assert!(!member_is_host("web.staging.12abc", "web.staging", None));
    assert!(member_is_host("info-test-host.7", "info-test-host", None));
}
