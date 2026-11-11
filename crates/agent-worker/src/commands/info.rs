//! Read-only view of live supervisors and queue depths.
//!
//! The command loads the application only to read its Redis URL and `Config`.
//! It does not register a worker and it does not write a heartbeat.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;

use simple_q_runtime::admin::{self, HeldTask, ProcessInfo, QueueStats, Snapshot, SupervisorInfo};
use simple_q_runtime::redis_api::{self, ConnectionManager};

const KWARG_VALUE_CHARS: usize = 8;

/// Print one snapshot and return. Exit 1 when this host has no live supervisor.
pub async fn run(
    app: String,
    factory: bool,
    all: bool,
    queue: Vec<String>,
    json: bool,
) -> Result<()> {
    let (url, settings) = crate::bridge::read_client(&app, factory)?;
    let mut conn = redis_api::connect(&url).await?;
    let host = simple_q_runtime::ids::hostname();
    let requested = !queue.is_empty();
    let queues = if requested {
        unique_names(queue)
    } else {
        let host_filter = if all { None } else { Some(host.as_str()) };
        discover_queues(&mut conn, &settings, host_filter).await?
    };
    let host_filter = if all { None } else { Some(host.as_str()) };
    let snapshot = admin::snapshot(&mut conn, &settings, &queues, host_filter)
        .await
        .context("failed to read the supervisor snapshot")?;
    let missing = !all && !snapshot.supervisors.iter().any(|item| item.host == host);
    if missing {
        eprintln!("no live supervisor on {host}");
        if has_rows(&snapshot, requested, all) {
            emit(&snapshot, json, all)?;
        }
        std::process::exit(1);
    }
    emit(&snapshot, json, all)?;
    Ok(())
}

fn has_rows(snapshot: &Snapshot, requested_queues: bool, all: bool) -> bool {
    requested_queues
        || !snapshot.supervisors.is_empty()
        || !snapshot.queues.is_empty()
        || (all && !snapshot.orphans.is_empty())
}

fn emit(snapshot: &Snapshot, json: bool, include_orphans: bool) -> Result<()> {
    if json {
        let text =
            serde_json::to_string_pretty(snapshot).context("failed to encode the snapshot")?;
        println!("{text}");
        return Ok(());
    }
    println!("{}", render_text(snapshot, include_orphans));
    Ok(())
}

/// Supervisors whose heartbeat is still inside the same window as `worker_alive`.
///
/// The queues to pass to `snapshot` are the `queues` field on those hashes.
/// Without `host`, every fresh supervisor contributes. With `host`, only that host.
async fn discover_queues(
    conn: &mut ConnectionManager,
    settings: &simple_q_runtime::config::Config,
    host_filter: Option<&str>,
) -> Result<Vec<String>> {
    let window = settings.orphan_window_secs();
    let (now_secs, _micros): (u64, u64) = redis::cmd("TIME")
        .query_async(conn)
        .await
        .context("failed to read Redis time")?;
    let rows: Vec<(String, f64)> = redis::cmd("ZRANGE")
        .arg(admin::SUPERVISORS_KEY)
        .arg(0i64)
        .arg(-1i64)
        .arg("WITHSCORES")
        .query_async(conn)
        .await
        .context("failed to list supervisors")?;
    let mut names = Vec::new();
    for (member, score) in rows {
        if !redis_api::heartbeat_is_fresh(Some(score), now_secs, window) {
            continue;
        }
        let fields: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(admin::supervisor_key(&member))
            .arg("host")
            .arg("hostname")
            .arg("pid")
            .arg("queues")
            .query_async(conn)
            .await
            .with_context(|| format!("failed to read supervisor {member}"))?;
        let stored_host = field(&fields, 0).or_else(|| field(&fields, 1));
        if let Some(host) = host_filter {
            if !member_is_host(&member, host, stored_host) {
                continue;
            }
        }
        if let Some(raw) = field(&fields, 3) {
            names.extend(parse_queue_field(raw));
        }
    }
    Ok(unique_names(names))
}

fn field(fields: &[Option<String>], index: usize) -> Option<&str> {
    fields.get(index).and_then(|value| value.as_deref())
}

fn member_is_host(member: &str, host: &str, stored_host: Option<&str>) -> bool {
    if stored_host == Some(host) {
        return true;
    }
    match member.rsplit_once('.') {
        Some((name, pid))
            if name == host && !pid.is_empty() && pid.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            true
        }
        _ => member == host,
    }
}

fn parse_queue_field(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if trimmed.starts_with('[') {
        if let Ok(names) = serde_json::from_str::<Vec<String>>(trimmed) {
            return names;
        }
    }
    trimmed
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect()
}

fn unique_names(names: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();
    for name in names {
        let name = name.trim().to_owned();
        if name.is_empty() || !seen.insert(name.clone()) {
            continue;
        }
        unique.push(name);
    }
    unique
}

fn render_text(snapshot: &Snapshot, include_orphans: bool) -> String {
    let mut parts = Vec::new();
    for supervisor in &snapshot.supervisors {
        parts.push(render_supervisor(supervisor, snapshot.taken_at));
    }
    if !snapshot.queues.is_empty() {
        parts.push(render_queues(&snapshot.queues));
    }
    if include_orphans && !snapshot.orphans.is_empty() {
        parts.push(format!(
            "orphans\n{}",
            render_slots(&snapshot.orphans, snapshot.taken_at)
        ));
    }
    parts.join("\n")
}

fn render_supervisor(supervisor: &SupervisorInfo, taken_at: u64) -> String {
    let live = supervisor
        .children
        .iter()
        .filter(|child| child.ended_at.is_none())
        .count();
    let mut lines = vec![
        format!(
            "{:<10}  {}  pid {}  up {}  simple-q {}",
            "supervisor",
            supervisor.host,
            supervisor.pid,
            format_uptime(supervisor.started_at, taken_at),
            display_version(&supervisor.version),
        ),
        format!(
            "{:<10}  {}  processes {live}/{}  concurrency {}  max_tasks {}",
            "queues",
            supervisor.queues.join(", "),
            supervisor.processes,
            supervisor.concurrency,
            supervisor.max_tasks,
        ),
        format!(
            "{:<10}  cgroup {} / {}  child soft {} / hard {}",
            "memory",
            bytes_or_dash(supervisor.cgroup_current),
            bytes_or_dash(supervisor.cgroup_max),
            bytes_or_dash(supervisor.child_soft),
            bytes_or_dash(supervisor.child_hard),
        ),
        format!(
            "{:<10}  recycled {}  soft {}  hard {}  restarts {}",
            "counters",
            supervisor.recycled,
            supervisor.drains,
            supervisor.kills,
            supervisor.restarts,
        ),
        render_slots(&supervisor.children, taken_at),
    ];
    if let Some(tasks) = render_tasks(supervisor, taken_at) {
        lines.push(tasks);
    }
    lines.join("\n")
}

fn render_slots(children: &[ProcessInfo], taken_at: u64) -> String {
    let mut lines = vec![slot_header()];
    for child in children {
        lines.push(slot_line(child, taken_at));
    }
    lines.join("\n")
}

fn slot_header() -> String {
    columns(&[
        ("SLOT", Some(4)),
        ("PID", Some(3)),
        ("STATE", Some(8)),
        ("REASON", Some(11)),
        ("RSS", Some(5)),
        ("TASKS", Some(7)),
        ("HB", None),
    ])
}

fn slot_line(child: &ProcessInfo, taken_at: u64) -> String {
    let held = if matches!(child.state.as_str(), "killed" | "exited") && !child.tasks.is_empty() {
        Some(child.tasks.len())
    } else {
        None
    };
    let slot = child
        .slot
        .map(|slot| slot.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let pid = child.pid.to_string();
    let rss = bytes_or_dash(child.rss);
    let tasks = task_budget(child.tasks_done, child.max_tasks);
    let hb = format_hb(child.last_seen, taken_at);
    let reason = child.reason.as_deref().unwrap_or("");
    let mut line = columns(&[
        (&slot, Some(4)),
        (&pid, Some(3)),
        (child.state.as_str(), Some(8)),
        (reason, Some(11)),
        (&rss, Some(5)),
        (&tasks, Some(7)),
        (&hb, held.map(|_| 4)),
    ]);
    if let Some(count) = held {
        line.push_str(&format!("  held {count}, waiting for requeue"));
    }
    line
}

fn render_tasks(supervisor: &SupervisorInfo, taken_at: u64) -> Option<String> {
    let mut rows = Vec::new();
    for child in &supervisor.children {
        for task in &child.tasks {
            rows.push(task_line(child.pid, task, taken_at));
        }
    }
    if rows.is_empty() {
        return None;
    }
    let mut lines = vec![columns(&[
        ("PID", Some(3)),
        ("QUEUE", Some(5)),
        ("ACTOR", Some(27)),
        ("KWARGS", Some(22)),
        ("TRY", Some(3)),
        ("DLV", Some(3)),
        ("RUN", None),
    ])];
    lines.extend(rows);
    Some(lines.join("\n"))
}

fn task_line(pid: u32, task: &HeldTask, taken_at: u64) -> String {
    let pid = pid.to_string();
    let kwargs = format_kwargs(&task.kwargs);
    let tries = format!("{}/{}", task.retries, task.max_retries);
    let deliveries = format_deliveries(task.deliveries);
    let run = format_run(task.locked_at, task.time_limit_ms, taken_at);
    columns(&[
        (&pid, Some(3)),
        (task.queue.as_str(), Some(5)),
        (task.actor_name.as_str(), Some(27)),
        (&kwargs, Some(22)),
        (&tries, Some(3)),
        (&deliveries, Some(3)),
        (&run, None),
    ])
}

fn render_queues(queues: &[QueueStats]) -> String {
    let mut lines = vec![columns(&[
        ("QUEUE", Some(5)),
        ("ACTIVE", Some(6)),
        ("SCHEDULED", Some(9)),
        ("INFLIGHT", Some(8)),
        ("DONE", Some(4)),
        ("DEAD", None),
    ])];
    for queue in queues {
        let active = queue.active.to_string();
        let scheduled = queue.scheduled.to_string();
        let inflight = queue.inflight.to_string();
        let done = queue.done.to_string();
        let dead = queue.dead.to_string();
        lines.push(columns(&[
            (queue.name.as_str(), Some(5)),
            (&active, Some(6)),
            (&scheduled, Some(9)),
            (&inflight, Some(8)),
            (&done, Some(4)),
            (&dead, None),
        ]));
    }
    lines.join("\n")
}

fn columns(cells: &[(&str, Option<usize>)]) -> String {
    let mut line = String::new();
    for (index, (text, width)) in cells.iter().enumerate() {
        if index > 0 {
            line.push_str("  ");
        }
        if let Some(width) = *width {
            line.push_str(&format!("{text:<width$}"));
        } else {
            line.push_str(text);
        }
    }
    line
}

fn task_budget(done: Option<u64>, max_tasks: Option<u64>) -> String {
    match (done, max_tasks) {
        (Some(done), Some(max_tasks)) => format!("{done}/{max_tasks}"),
        (Some(done), None) => format!("{done}/-"),
        (None, Some(max_tasks)) => format!("-/{max_tasks}"),
        (None, None) => "-".to_owned(),
    }
}

fn format_kwargs(kwargs: &Value) -> String {
    let Some(object) = kwargs.as_object() else {
        return truncate_value(&kwargs.to_string());
    };
    object
        .iter()
        .map(|(key, value)| {
            let rendered = match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            format!("{key}={}", truncate_value(&rendered))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_value(value: &str) -> String {
    let mut chars = value.chars();
    let head: String = chars.by_ref().take(KWARG_VALUE_CHARS).collect();
    if chars.next().is_some() {
        format!("{head}\u{2026}")
    } else {
        head
    }
}

fn format_deliveries(deliveries: u64) -> String {
    if deliveries == 0 {
        "-".to_owned()
    } else {
        deliveries.to_string()
    }
}

fn format_run(locked_at: Option<u64>, time_limit_ms: u64, taken_at: u64) -> String {
    let Some(locked_at) = locked_at else {
        return "-".to_owned();
    };
    let elapsed = taken_at.saturating_sub(locked_at);
    let limit = time_limit_ms / 1000;
    format!("{elapsed}s/{limit}s")
}

fn format_hb(last_seen: Option<u64>, taken_at: u64) -> String {
    match last_seen {
        Some(seen) => format!("{}s", taken_at.saturating_sub(seen)),
        None => "-".to_owned(),
    }
}

fn bytes_or_dash(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) => format_bytes(bytes),
        None => "-".to_owned(),
    }
}

/// 1024-based. An exact GiB, MiB, or KiB stays a whole number (`4G`, `1500M`).
/// Anything else uses one decimal at 10 or above and two below, dropping a trailing zero.
fn format_bytes(bytes: u64) -> String {
    const UNITS: [(u64, &str); 3] = [(1024 * 1024 * 1024, "G"), (1024 * 1024, "M"), (1024, "K")];
    for (unit, suffix) in UNITS {
        if bytes >= unit && bytes % unit == 0 {
            return format!("{}{suffix}", bytes / unit);
        }
    }
    for (unit, suffix) in UNITS {
        if bytes >= unit {
            let value = bytes as f64 / unit as f64;
            let text = if value >= 10.0 {
                format!("{value:.1}")
            } else {
                format!("{value:.2}")
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_owned()
            };
            return format!("{text}{suffix}");
        }
    }
    if bytes == 0 {
        "0".to_owned()
    } else {
        format!("{bytes}B")
    }
}

fn format_uptime(started_at: u64, taken_at: u64) -> String {
    let secs = taken_at.saturating_sub(started_at);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m", secs / 60)
    } else {
        let hours = secs / 3_600;
        let minutes = (secs % 3_600) / 60;
        format!("{hours}h{minutes}m")
    }
}

/// Cargo writes `1.0.0-rc3`. The text line prints `1.0.0rc3`.
fn display_version(version: &str) -> String {
    version.replacen("-rc", "rc", 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use simple_q_runtime::admin::{HeldTask, ProcessInfo, QueueStats, Snapshot, SupervisorInfo};

    #[test]
    fn format_bytes_uses_binary_units() {
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
        assert_eq!(format_uptime(1_000, 1_045), "45s");
        assert_eq!(format_uptime(1_000, 1_000 + 38 * 60), "38m");
        assert_eq!(format_uptime(1_000, 1_000 + 2 * 3_600 + 5 * 60), "2h5m");
        assert_eq!(display_version("1.0.0-rc3"), "1.0.0rc3");
    }

    #[test]
    fn one_supervisor_block_lists_children_tasks_and_queue_depth() {
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
                version: "1.0.0-rc3".to_owned(),
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
        let expected = "\
supervisor  worker-quota-6f8b9c7d4-x2k9p  pid 1  up 38m  simple-q 1.0.0rc3
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
        assert_eq!(text, expected);
    }

    #[test]
    fn queue_fields_accept_json_and_commas() {
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
            deliveries: 1,
        }
    }

    #[test]
    fn member_is_host_requires_the_pid_suffix() {
        assert!(member_is_host("web.staging.12", "web.staging", None));
        assert!(!member_is_host("web.staging.12", "web", None));
        assert!(member_is_host("web.staging.12", "web", Some("web")));
        assert!(!member_is_host("web.staging.12abc", "web.staging", None));
        assert!(member_is_host("info-test-host.7", "info-test-host", None));
    }
}
