//! Supervisor heartbeats and the admin snapshot.
//!
//! Queue key names come from `apalis_redis::Config`. A snapshot reads counters,
//! heartbeats, inflight sets, and the hashes for tasks those sets are holding.
//! It does not walk active, scheduled, or dead lists. A process still shows
//! held tasks for every queue named on its supervisor record.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use apalis::prelude::WorkerContext;
use apalis_redis::Config as QueueConfig;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::job::Job;
use crate::redis_api::{self, delivery_key};

/// Sorted set of supervisor members. The score is the last Redis `TIME`.
pub const SUPERVISORS_KEY: &str = "simple_q:supervisors";

const CHILD_FIELD: &str = "child:";
const TASKS_FIELD: &str = "tasks:";
/// `wait` status for `SIGKILL`. The supervisor did not choose this signal.
const SIGKILL: i32 = 9;

/// `{host}.{pid}`. The host may already contain dots.
pub fn supervisor_member(host: &str, pid: u32) -> String {
    format!("{host}.{pid}")
}

/// Hash of one supervisor heartbeat. `member` is [`supervisor_member`].
pub fn supervisor_key(member: &str) -> String {
    format!("simple_q:supervisor:{member}")
}

/// How long a supervisor hash and a finished child row stay visible.
///
/// Two heartbeat windows, and at least one second. `EXPIRE 0` would delete the
/// hash on the same command that wrote it.
pub fn supervisor_ttl_secs(settings: &Config) -> u64 {
    settings
        .heartbeat_secs
        .saturating_mul(settings.missed_heartbeats)
        .saturating_mul(2)
        .max(1)
}

/// Signal the supervisor has sent to a child. `None` means it has not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentSignal {
    None,
    Usr1(DrainCause),
    Kill(KillCause),
}

/// Why the supervisor asked a child to stop taking work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainCause {
    MemorySoft,
    MemoryRatio,
    Shutdown,
}

/// Why the supervisor killed a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillCause {
    MemoryHard,
    MemoryRatio,
}

/// What the snapshot shows for a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildState {
    Running,
    Draining,
    Exited,
    Killed,
}

/// Why a child is draining, exited, or was killed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildReason {
    MemorySoft,
    MemoryRatio,
    Shutdown,
    MaxTasks,
    MemoryHard,
    PingBudget,
    Sigkill,
    Crash,
}

impl ChildState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Exited => "exited",
            Self::Killed => "killed",
        }
    }
}

impl ChildReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemorySoft => "memory_soft",
            Self::MemoryRatio => "memory_ratio",
            Self::Shutdown => "shutdown",
            Self::MaxTasks => "max_tasks",
            Self::MemoryHard => "memory_hard",
            Self::PingBudget => "ping_budget",
            Self::Sigkill => "sigkill",
            Self::Crash => "crash",
        }
    }
}

impl DrainCause {
    fn reason(self) -> ChildReason {
        match self {
            Self::MemorySoft => ChildReason::MemorySoft,
            Self::MemoryRatio => ChildReason::MemoryRatio,
            Self::Shutdown => ChildReason::Shutdown,
        }
    }
}

impl KillCause {
    fn reason(self) -> ChildReason {
        match self {
            Self::MemoryHard => ChildReason::MemoryHard,
            Self::MemoryRatio => ChildReason::MemoryRatio,
        }
    }
}

/// Classify a child from the signal the supervisor sent and the wait status.
///
/// Both `exit_code` and `term_signal` are `None` until `wait` returns. A
/// `Kill` stays that cause after wait. Exit code 2 is the ping budget. Exit
/// code 0 after `SIGUSR1` keeps that drain cause. Exit code 0 with no drain is
/// the max-task recycle. `SIGKILL` that the supervisor did not send is
/// `sigkill`. Every other status is a crash.
pub fn classify_child(
    signal: SentSignal,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
) -> (ChildState, Option<ChildReason>) {
    if exit_code.is_none() && term_signal.is_none() {
        return match signal {
            SentSignal::None => (ChildState::Running, None),
            SentSignal::Usr1(cause) => (ChildState::Draining, Some(cause.reason())),
            SentSignal::Kill(cause) => (ChildState::Killed, Some(cause.reason())),
        };
    }
    if let SentSignal::Kill(cause) = signal {
        return (ChildState::Killed, Some(cause.reason()));
    }
    if exit_code == Some(2) {
        return (ChildState::Killed, Some(ChildReason::PingBudget));
    }
    if term_signal == Some(SIGKILL) {
        return (ChildState::Killed, Some(ChildReason::Sigkill));
    }
    if exit_code == Some(0) {
        return match signal {
            SentSignal::Usr1(cause) => (ChildState::Exited, Some(cause.reason())),
            _ => (ChildState::Exited, Some(ChildReason::MaxTasks)),
        };
    }
    (ChildState::Killed, Some(ChildReason::Crash))
}

/// Totals the supervisor publishes on its heartbeat.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    pub recycled: u64,
    pub drains: u64,
    pub kills: u64,
    pub restarts: u64,
}

/// One child the supervisor is watching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildTrack {
    pub slot: u32,
    pub signal: SentSignal,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub rss: Option<u64>,
    pub started_at: u64,
    pub ended_at: Option<u64>,
}

impl ChildTrack {
    /// A child that has just been spawned. No signal, no rss, both clocks 0.
    pub fn start(slot: u32) -> Self {
        Self {
            slot,
            signal: SentSignal::None,
            exit_code: None,
            term_signal: None,
            rss: None,
            started_at: 0,
            ended_at: None,
        }
    }

    /// Record a signal. A second drain is ignored. Anything after `Kill` is ignored.
    ///
    /// The first soft or ratio drain counts. Shutdown does not. `Kill` counts
    /// a kill and a restart immediately, before `wait`.
    pub fn apply_signal(&mut self, signal: SentSignal, counters: &mut Counters) {
        if matches!(self.signal, SentSignal::Kill(_)) {
            return;
        }
        match signal {
            SentSignal::None => {}
            SentSignal::Usr1(_) if matches!(self.signal, SentSignal::Usr1(_)) => {}
            SentSignal::Usr1(cause) => {
                if matches!(cause, DrainCause::MemorySoft | DrainCause::MemoryRatio) {
                    counters.drains = counters.drains.saturating_add(1);
                }
                self.signal = SentSignal::Usr1(cause);
            }
            SentSignal::Kill(cause) => {
                counters.kills = counters.kills.saturating_add(1);
                counters.restarts = counters.restarts.saturating_add(1);
                self.signal = SentSignal::Kill(cause);
            }
        }
    }

    /// Record `wait` once. A second call does not move the counters.
    ///
    /// An exited child counts as recycled. A killed child counts a restart
    /// only when `apply_signal` did not already count the `Kill`. Ping-budget,
    /// sigkill, and crash do not count as kills.
    pub fn apply_exit(
        &mut self,
        code: Option<i32>,
        term: Option<i32>,
        now: u64,
        counters: &mut Counters,
    ) {
        if self.ended_at.is_some() {
            return;
        }
        self.exit_code = code;
        self.term_signal = term;
        self.ended_at = Some(now);
        let (state, _) = classify_child(self.signal, self.exit_code, self.term_signal);
        match state {
            ChildState::Exited => {
                counters.recycled = counters.recycled.saturating_add(1);
            }
            ChildState::Killed => {
                if !matches!(self.signal, SentSignal::Kill(_)) {
                    counters.restarts = counters.restarts.saturating_add(1);
                }
            }
            ChildState::Running | ChildState::Draining => {}
        }
    }

    /// The row written into the supervisor hash.
    ///
    /// `exit` prefers the terminating signal over the exit code. `pid` stays 0
    /// until the caller sets the operating-system pid. `publish_supervisor`
    /// uses that pid as the `child:{pid}` field name.
    pub fn record(&self) -> ChildRecord {
        let (state, reason) = classify_child(self.signal, self.exit_code, self.term_signal);
        ChildRecord {
            pid: 0,
            slot: self.slot,
            state: state.as_str().to_owned(),
            reason: reason.map(|reason| reason.as_str().to_owned()),
            rss: self.rss,
            started_at: self.started_at,
            ended_at: self.ended_at,
            exit: self.term_signal.or(self.exit_code),
            tasks_done: None,
            max_tasks: None,
        }
    }
}

/// One child row stored at `child:{pid}` in the supervisor hash.
///
/// `pid` is the hash field suffix. It is not part of the JSON. `record` leaves
/// it at 0; set it to the operating-system pid before publish. `reason`, `rss`,
/// `ended_at`, and `exit` are omitted when they are absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildRecord {
    /// Operating-system pid used as the `child:{pid}` field name.
    #[serde(default, skip_serializing)]
    pub pid: u32,
    pub slot: u32,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rss: Option<u64>,
    pub started_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<i32>,
    /// Last completed-task count. Kept after the worker metadata hash is deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks_done: Option<u64>,
    /// Task budget for this process. Kept after the worker metadata hash is deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tasks: Option<u64>,
}

/// A finished row is due once `now - ended_at` reaches `retain_secs`.
///
/// A row that is still running has no `ended_at` and is kept.
pub fn finished_row_expired(ended_at: Option<u64>, now: u64, retain_secs: u64) -> bool {
    match ended_at {
        Some(ended) => now.saturating_sub(ended) >= retain_secs,
        None => false,
    }
}

/// Drop finished rows that have been kept for `retain_secs`.
pub fn prune_finished(children: &mut Vec<ChildRecord>, now: u64, retain_secs: u64) {
    children.retain(|child| !finished_row_expired(child.ended_at, now, retain_secs));
}

/// Heartbeat body written by the supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorRecord {
    pub host: String,
    pub pid: u32,
    pub started_at: u64,
    pub version: String,
    pub queues: Vec<String>,
    pub processes: u32,
    pub concurrency: u64,
    pub max_tasks: u64,
    pub child_soft: Option<u64>,
    pub child_hard: Option<u64>,
    pub cgroup_current: Option<u64>,
    pub cgroup_max: Option<u64>,
    pub recycled: u64,
    pub drains: u64,
    pub kills: u64,
    pub restarts: u64,
    pub children: Vec<ChildRecord>,
}

/// Drop the supervisor hash and its sorted-set member after an orderly stop.
///
/// The hash TTL is the backstop for a process that never reaches this call.
/// A heartbeat that runs after this delete would publish the record again.
pub async fn forget_supervisor(conn: &mut ConnectionManager, host: &str, pid: u32) -> Result<()> {
    let member = supervisor_member(host, pid);
    let key = supervisor_key(&member);
    redis::pipe()
        .cmd("DEL")
        .arg(&key)
        .cmd("ZREM")
        .arg(SUPERVISORS_KEY)
        .arg(&member)
        .query_async::<()>(conn)
        .await
        .context("failed to remove a supervisor")?;
    Ok(())
}

/// Publish one supervisor heartbeat.
///
/// The score and `last_seen` are Redis `TIME`. Finished children older than
/// two windows are left out of this write, and `child:*` fields that are no
/// longer in the record are removed. The hash itself is not deleted, so a
/// reader does not see a gap between the delete and the next write.
pub async fn publish_supervisor(
    conn: &mut ConnectionManager,
    settings: &Config,
    record: &SupervisorRecord,
) -> Result<()> {
    let member = supervisor_member(&record.host, record.pid);
    let key = supervisor_key(&member);
    let mut pipe = redis::pipe();
    pipe.cmd("TIME").cmd("HGETALL").arg(&key);
    let read: Vec<redis::Value> = pipe
        .query_async(conn)
        .await
        .context("failed to read Redis time")?;
    if read.len() != 2 {
        bail!("supervisor publish expected TIME and the current hash");
    }
    let now = redis_time(&read[0])?;
    let existing = hash_map(&read[1]);
    let retain = supervisor_ttl_secs(settings);
    let mut children = record.children.clone();
    for child in &mut children {
        restore_child_progress(&existing, child, record.max_tasks);
    }
    prune_finished(&mut children, now, retain);
    let mut by_pid: BTreeMap<u32, &ChildRecord> = BTreeMap::new();
    for child in &children {
        by_pid.insert(child.pid, child);
    }
    let mut fields = vec![
        ("host".to_owned(), record.host.clone()),
        ("pid".to_owned(), record.pid.to_string()),
        ("started_at".to_owned(), record.started_at.to_string()),
        ("version".to_owned(), record.version.clone()),
        ("queues".to_owned(), record.queues.join(",")),
        ("processes".to_owned(), record.processes.to_string()),
        ("concurrency".to_owned(), record.concurrency.to_string()),
        ("max_tasks".to_owned(), record.max_tasks.to_string()),
        ("child_soft".to_owned(), opt_u64(record.child_soft)),
        ("child_hard".to_owned(), opt_u64(record.child_hard)),
        ("last_seen".to_owned(), now.to_string()),
        ("cgroup_current".to_owned(), opt_u64(record.cgroup_current)),
        ("cgroup_max".to_owned(), opt_u64(record.cgroup_max)),
        ("recycled".to_owned(), record.recycled.to_string()),
        ("drains".to_owned(), record.drains.to_string()),
        ("kills".to_owned(), record.kills.to_string()),
        ("restarts".to_owned(), record.restarts.to_string()),
    ];
    for (pid, child) in &by_pid {
        let json = serde_json::to_string(child).context("failed to encode a child record")?;
        fields.push((format!("{CHILD_FIELD}{pid}"), json));
    }
    let keep: BTreeSet<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
    let stale: Vec<&str> = existing
        .keys()
        .filter(|name| {
            if name.starts_with(CHILD_FIELD) && !keep.contains(name.as_str()) {
                return true;
            }
            match name.strip_prefix(TASKS_FIELD) {
                Some(pid) => pid
                    .parse::<u32>()
                    .ok()
                    .is_some_and(|pid| !by_pid.contains_key(&pid)),
                None => false,
            }
        })
        .map(String::as_str)
        .collect();
    let ttl = supervisor_ttl_secs(settings);
    let cutoff = now.saturating_sub(settings.orphan_window_secs());
    let mut write = redis::pipe();
    {
        let cmd = write.cmd("HSET");
        cmd.arg(&key);
        for (name, value) in &fields {
            cmd.arg(name).arg(value);
        }
    }
    if !stale.is_empty() {
        let cmd = write.cmd("HDEL");
        cmd.arg(&key);
        for name in &stale {
            cmd.arg(*name);
        }
    }
    write
        .cmd("EXPIRE")
        .arg(&key)
        .arg(ttl)
        .cmd("ZADD")
        .arg(SUPERVISORS_KEY)
        .arg(now)
        .arg(&member)
        .cmd("ZREMRANGEBYSCORE")
        .arg(SUPERVISORS_KEY)
        .arg("-inf")
        .arg(format!("({cutoff}"));
    write
        .query_async::<()>(conn)
        .await
        .context("failed to publish a supervisor")?;
    Ok(())
}

/// Record that a worker process started. `slot` is written only when it is known.
pub async fn note_worker_start(
    conn: &mut ConnectionManager,
    settings: &Config,
    queue: &str,
    name: &str,
    ppid: u32,
    slot: Option<u32>,
    max_tasks: u64,
) -> Result<()> {
    let config = redis_api::queue_config(queue, settings)?;
    let key = worker_metadata(&config, name);
    let now = redis_now(conn).await?;
    let mut cmd = redis::cmd("HSET");
    cmd.arg(&key)
        .arg("ppid")
        .arg(ppid)
        .arg("started_at")
        .arg(now)
        .arg("max_tasks")
        .arg(max_tasks);
    if let Some(slot) = slot {
        cmd.arg("slot").arg(slot);
    }
    cmd.query_async::<()>(conn)
        .await
        .context("failed to record worker start")?;
    Ok(())
}

/// Remember `tasks_done` on the supervisor hash.
///
/// An orderly exit deletes the worker metadata hash. The supervisor copies
/// this field into `child:{pid}` on its next publish, so the exited row still
/// has a task count.
pub async fn note_child_tasks(
    conn: &mut ConnectionManager,
    host: &str,
    supervisor_pid: u32,
    pid: u32,
    tasks_done: u64,
) -> Result<()> {
    let key = supervisor_key(&supervisor_member(host, supervisor_pid));
    redis::cmd("HSET")
        .arg(&key)
        .arg(format!("{TASKS_FIELD}{pid}"))
        .arg(tasks_done)
        .query_async::<()>(conn)
        .await
        .context("failed to record child tasks_done")?;
    Ok(())
}

/// Record how many tasks this worker has finished.
pub async fn note_tasks_done(
    conn: &mut ConnectionManager,
    settings: &Config,
    queue: &str,
    name: &str,
    tasks_done: u64,
) -> Result<()> {
    let config = redis_api::queue_config(queue, settings)?;
    let key = worker_metadata(&config, name);
    redis::cmd("HSET")
        .arg(&key)
        .arg("tasks_done")
        .arg(tasks_done)
        .query_async::<()>(conn)
        .await
        .context("failed to record tasks_done")?;
    Ok(())
}

/// One read of the queues and the processes working them.
///
/// Optional fields serialize as JSON null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub taken_at: u64,
    pub queues: Vec<QueueStats>,
    pub supervisors: Vec<SupervisorInfo>,
    pub orphans: Vec<ProcessInfo>,
}

/// Counts for one queue. `inflight` is the sum of every worker's inflight set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    pub name: String,
    pub active: u64,
    pub scheduled: u64,
    pub inflight: u64,
    pub done: u64,
    pub dead: u64,
    pub workers: u64,
    pub workers_alive: u64,
}

/// One live supervisor and the children attached to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SupervisorInfo {
    pub host: String,
    pub pid: u32,
    pub started_at: u64,
    pub last_seen: u64,
    pub version: String,
    pub queues: Vec<String>,
    pub processes: u32,
    pub concurrency: u64,
    pub max_tasks: u64,
    pub child_soft: Option<u64>,
    pub child_hard: Option<u64>,
    pub cgroup_current: Option<u64>,
    pub cgroup_max: Option<u64>,
    pub recycled: u64,
    pub drains: u64,
    pub kills: u64,
    pub restarts: u64,
    pub children: Vec<ProcessInfo>,
}

/// One operating-system process, either under a supervisor or orphaned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub host: String,
    pub pid: u32,
    pub ppid: Option<u32>,
    pub slot: Option<u32>,
    pub state: String,
    pub reason: Option<String>,
    pub exit: Option<i32>,
    pub rss: Option<u64>,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub tasks_done: Option<u64>,
    pub max_tasks: Option<u64>,
    pub workers: Vec<String>,
    pub last_seen: Option<u64>,
    pub alive: bool,
    pub tasks: Vec<HeldTask>,
}

/// A task id currently in some worker's inflight set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeldTask {
    pub queue: String,
    pub task_id: String,
    pub message_id: String,
    pub actor_name: String,
    pub kwargs: serde_json::Value,
    pub retries: u32,
    pub max_retries: u32,
    pub time_limit_ms: u64,
    /// Absolute lease time. Apalis overwrites this on every heartbeat renew.
    pub locked_at: Option<u64>,
    /// Redis `TIME` seconds when this delivery was taken. Renew does not change it.
    pub started_at: Option<u64>,
    pub deliveries: u64,
}

/// Read the admin snapshot.
///
/// Queue counters and heartbeats, then supervisor and worker hashes plus
/// inflight members, then each held task's payload, `locked_at`, and
/// `started_at`. The delivery counter is read after that payload, because its
/// key needs `message_id` and `retries`. `host` limits supervisors, their
/// children, and orphans. Queue counts stay limited to `queues`. A process
/// still shows held tasks for every queue on its supervisor record, including
/// a queue the caller did not pass. A missing task payload still produces a
/// [`HeldTask`] and does not fail the snapshot.
pub async fn snapshot(
    conn: &mut ConnectionManager,
    settings: &Config,
    queues: &[String],
    host: Option<&str>,
) -> Result<Snapshot> {
    let window = settings.orphan_window_secs();
    let mut prepared = Vec::with_capacity(queues.len());
    for queue in queues {
        let config = redis_api::queue_config(queue, settings)?;
        prepared.push((queue.clone(), config));
    }
    let (now, members, drafts) = read_overview(conn, &prepared).await?;
    let live: Vec<SupHead> = members
        .into_iter()
        .filter(|member| {
            redis_api::heartbeat_is_fresh(Some(member.score), now, window)
                && host_selected(&member.host, host)
        })
        .collect();
    let (sup_fields, mut drafts) = read_bodies(conn, &prepared, &live, drafts).await?;
    let loaded_sups = live
        .into_iter()
        .zip(sup_fields)
        .map(|(head, fields)| LoadedSup {
            host: head.host,
            pid: head.pid,
            score: head.score,
            fields,
        })
        .collect::<Vec<_>>();
    let mut held_configs = prepared;
    attach_unrequested_queues(conn, settings, &loaded_sups, &mut held_configs, &mut drafts).await?;
    let held_keys = tree_task_keys(&drafts, host);
    let held = read_held(conn, &held_configs, &held_keys).await?;
    Ok(build_snapshot(
        now,
        window,
        host,
        drafts,
        loaded_sups,
        &held,
    ))
}

struct SupHead {
    host: String,
    pid: u32,
    score: f64,
}

struct QueueDraft {
    name: String,
    active: u64,
    scheduled: u64,
    done: u64,
    dead: u64,
    workers: Vec<LoadedWorker>,
    /// `false` for a queue loaded only so a supervisor's process can show its tasks.
    counted: bool,
}

struct LoadedWorker {
    name: String,
    score: f64,
    meta: BTreeMap<String, String>,
    inflight: Vec<String>,
}

struct LoadedSup {
    host: String,
    pid: u32,
    score: f64,
    fields: BTreeMap<String, String>,
}

/// Apalis 1.1.1 stores a new done or dead key as a sorted set scored by
/// completion time. A key that is already a set stays a set.
const TERMINAL_CARD: &str = r#"
local key_type = redis.call("TYPE", KEYS[1])
if type(key_type) == "table" then
    key_type = key_type["ok"]
end
if key_type == "zset" then
    return redis.call("ZCARD", KEYS[1])
end
if key_type == "set" then
    return redis.call("SCARD", KEYS[1])
end
return 0
"#;

fn terminal_card(pipe: &mut redis::Pipeline, key: &str) {
    pipe.cmd("EVAL").arg(TERMINAL_CARD).arg(1).arg(key);
}

async fn read_overview(
    conn: &mut ConnectionManager,
    prepared: &[(String, QueueConfig)],
) -> Result<(u64, Vec<SupHead>, Vec<QueueDraft>)> {
    let mut pipe = redis::pipe();
    let mut expected = 0usize;
    pipe.cmd("TIME");
    let time_at = expected;
    expected += 1;
    pipe.cmd("ZRANGEBYSCORE")
        .arg(SUPERVISORS_KEY)
        .arg("-inf")
        .arg("+inf")
        .arg("WITHSCORES");
    let supers_at = expected;
    expected += 1;
    let mut slots = Vec::with_capacity(prepared.len());
    for (_, config) in prepared {
        pipe.cmd("LLEN").arg(config.active_jobs_list());
        let active_at = expected;
        expected += 1;
        pipe.cmd("ZCARD").arg(config.scheduled_jobs_set());
        let scheduled_at = expected;
        expected += 1;
        terminal_card(&mut pipe, &config.done_jobs_set());
        let done_at = expected;
        expected += 1;
        terminal_card(&mut pipe, &config.dead_jobs_set());
        let dead_at = expected;
        expected += 1;
        pipe.cmd("ZRANGE")
            .arg(config.workers_set())
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES");
        let workers_at = expected;
        expected += 1;
        slots.push((active_at, scheduled_at, done_at, dead_at, workers_at));
    }
    let values = query_values(conn, &pipe, expected, "failed to read queue counters").await?;
    let now = redis_time(&values[time_at])?;
    let members = scored_members(&values[supers_at])
        .into_iter()
        .filter_map(|(member, score)| {
            let (host, pid) = parse_supervisor_member(&member)?;
            Some(SupHead { host, pid, score })
        })
        .collect();
    let mut drafts = Vec::with_capacity(prepared.len());
    for (index, (name, _)) in prepared.iter().enumerate() {
        let (active_at, scheduled_at, done_at, dead_at, workers_at) = slots[index];
        let workers = scored_members(&values[workers_at])
            .into_iter()
            .map(|(worker_name, score)| LoadedWorker {
                name: worker_name,
                score,
                meta: BTreeMap::new(),
                inflight: Vec::new(),
            })
            .collect();
        drafts.push(QueueDraft {
            name: name.clone(),
            active: value_u64(&values[active_at]),
            scheduled: value_u64(&values[scheduled_at]),
            done: value_u64(&values[done_at]),
            dead: value_u64(&values[dead_at]),
            workers,
            counted: true,
        });
    }
    Ok((now, members, drafts))
}

async fn read_bodies(
    conn: &mut ConnectionManager,
    prepared: &[(String, QueueConfig)],
    supers: &[SupHead],
    mut drafts: Vec<QueueDraft>,
) -> Result<(Vec<BTreeMap<String, String>>, Vec<QueueDraft>)> {
    let mut pipe = redis::pipe();
    let mut expected = 0usize;
    for head in supers {
        let member = supervisor_member(&head.host, head.pid);
        pipe.cmd("HGETALL").arg(supervisor_key(&member));
        expected += 1;
    }
    let worker_base = expected;
    for (queue_index, draft) in drafts.iter().enumerate() {
        let config = &prepared[queue_index].1;
        for worker in &draft.workers {
            pipe.cmd("HGETALL")
                .arg(worker_metadata(config, &worker.name));
            expected += 1;
            pipe.cmd("SMEMBERS")
                .arg(worker_inflight(config, &worker.name));
            expected += 1;
        }
    }
    if expected == 0 {
        return Ok((Vec::new(), drafts));
    }
    let values = query_values(conn, &pipe, expected, "failed to read worker hashes").await?;
    let sup_fields = values[..supers.len()]
        .iter()
        .map(hash_map)
        .collect::<Vec<_>>();
    let mut cursor = worker_base;
    for draft in &mut drafts {
        for worker in &mut draft.workers {
            worker.meta = hash_map(&values[cursor]);
            cursor += 1;
            worker.inflight = string_list(&values[cursor]);
            cursor += 1;
        }
    }
    Ok((sup_fields, drafts))
}

/// Load workers for supervisor queues the caller did not ask to count.
///
/// A matching worker is one whose host is the supervisor's host and whose
/// `ppid` is that supervisor, or whose pid is already a child row. Other
/// workers on that queue are left out, so they do not become orphans.
async fn attach_unrequested_queues(
    conn: &mut ConnectionManager,
    settings: &Config,
    supers: &[LoadedSup],
    configs: &mut Vec<(String, QueueConfig)>,
    drafts: &mut Vec<QueueDraft>,
) -> Result<()> {
    let known: BTreeSet<String> = configs.iter().map(|(name, _)| name.clone()).collect();
    let mut extra: BTreeMap<String, Vec<QueueOwner>> = BTreeMap::new();
    for sup in supers {
        let children = child_pid_set(&sup.fields);
        for queue in split_queues(sup.fields.get("queues").map(String::as_str).unwrap_or("")) {
            if known.contains(&queue) {
                continue;
            }
            extra.entry(queue).or_default().push(QueueOwner {
                host: sup.host.clone(),
                pid: sup.pid,
                children: children.clone(),
            });
        }
    }
    for (queue, owners) in extra {
        let Ok(config) = redis_api::queue_config(&queue, settings) else {
            continue;
        };
        let workers = load_queue_workers(conn, &config).await?;
        let workers: Vec<LoadedWorker> = workers
            .into_iter()
            .filter(|worker| worker_on_supervisor(worker, &owners))
            .collect();
        if workers.is_empty() {
            continue;
        }
        configs.push((queue.clone(), config));
        drafts.push(QueueDraft {
            name: queue,
            active: 0,
            scheduled: 0,
            done: 0,
            dead: 0,
            workers,
            counted: false,
        });
    }
    Ok(())
}

struct QueueOwner {
    host: String,
    pid: u32,
    children: BTreeSet<u32>,
}

fn child_pid_set(fields: &BTreeMap<String, String>) -> BTreeSet<u32> {
    child_records(fields)
        .into_iter()
        .map(|child| child.pid)
        .filter(|pid| *pid != 0)
        .collect()
}

fn worker_on_supervisor(worker: &LoadedWorker, owners: &[QueueOwner]) -> bool {
    let Some(parsed) = parse_worker_name(&worker.name) else {
        return false;
    };
    owners.iter().any(|owner| {
        if owner.host != parsed.host {
            return false;
        }
        let ppid = worker
            .meta
            .get("ppid")
            .and_then(|text| text.parse::<u32>().ok());
        ppid == Some(owner.pid) || owner.children.contains(&parsed.pid)
    })
}

async fn load_queue_workers(
    conn: &mut ConnectionManager,
    config: &QueueConfig,
) -> Result<Vec<LoadedWorker>> {
    let listed: redis::Value = redis::cmd("ZRANGE")
        .arg(config.workers_set())
        .arg(0)
        .arg(-1)
        .arg("WITHSCORES")
        .query_async(conn)
        .await
        .context("failed to read workers for task attachment")?;
    let scored = scored_members(&listed);
    if scored.is_empty() {
        return Ok(Vec::new());
    }
    let mut pipe = redis::pipe();
    for (name, _) in &scored {
        pipe.cmd("HGETALL").arg(worker_metadata(config, name));
        pipe.cmd("SMEMBERS").arg(worker_inflight(config, name));
    }
    let values = query_values(
        conn,
        &pipe,
        scored.len() * 2,
        "failed to read workers for task attachment",
    )
    .await?;
    let mut workers = Vec::with_capacity(scored.len());
    for (index, (name, score)) in scored.into_iter().enumerate() {
        workers.push(LoadedWorker {
            name,
            score,
            meta: hash_map(&values[index * 2]),
            inflight: string_list(&values[index * 2 + 1]),
        });
    }
    Ok(workers)
}

async fn read_held(
    conn: &mut ConnectionManager,
    prepared: &[(String, QueueConfig)],
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), HeldTask>> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut pipe = redis::pipe();
    for (queue, task_id) in keys {
        let Some(config) = config_for(prepared, queue) else {
            bail!("held task queue {queue} is not in this snapshot");
        };
        pipe.cmd("HGET").arg(config.job_data_hash()).arg(task_id);
        pipe.cmd("HMGET")
            .arg(format!("{}:{task_id}", config.job_meta_hash()))
            .arg("locked_at")
            .arg("started_at");
    }
    let values = query_values(conn, &pipe, keys.len() * 2, "failed to read held tasks").await?;
    let mut tasks = BTreeMap::new();
    let mut deliveries = Vec::new();
    for (index, (queue, task_id)) in keys.iter().enumerate() {
        let payload = value_string(&values[index * 2]);
        let locked = nth_string(&values[index * 2 + 1], 0);
        let started = nth_string(&values[index * 2 + 1], 1);
        let task = held_task_from_payload(
            queue,
            task_id,
            payload.as_deref(),
            locked.as_deref(),
            started.as_deref(),
            0,
        );
        if !task.message_id.is_empty() {
            deliveries.push((
                queue.clone(),
                task_id.clone(),
                delivery_key(&task.message_id, task.retries),
            ));
        }
        tasks.insert((queue.clone(), task_id.clone()), task);
    }
    if deliveries.is_empty() {
        return Ok(tasks);
    }
    let mut pipe = redis::pipe();
    for (_, _, key) in &deliveries {
        pipe.cmd("GET").arg(key);
    }
    let counts = query_values(
        conn,
        &pipe,
        deliveries.len(),
        "failed to read delivery counters",
    )
    .await?;
    for (index, (queue, task_id, _)) in deliveries.iter().enumerate() {
        if let Some(task) = tasks.get_mut(&(queue.clone(), task_id.clone())) {
            task.deliveries = value_string(&counts[index])
                .and_then(|text| parse_u64_loose(&text))
                .unwrap_or(0);
        }
    }
    Ok(tasks)
}

fn tree_task_keys(queues: &[QueueDraft], host: Option<&str>) -> Vec<(String, String)> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for queue in queues {
        for worker in &queue.workers {
            let Some(parsed) = parse_worker_name(&worker.name) else {
                continue;
            };
            if !host_selected(&parsed.host, host) {
                continue;
            }
            for task_id in &worker.inflight {
                if seen.insert((queue.name.clone(), task_id.clone())) {
                    keys.push((queue.name.clone(), task_id.clone()));
                }
            }
        }
    }
    keys
}

fn build_snapshot(
    now: u64,
    window: u64,
    host: Option<&str>,
    queues: Vec<QueueDraft>,
    supervisors: Vec<LoadedSup>,
    held: &BTreeMap<(String, String), HeldTask>,
) -> Snapshot {
    let queue_stats = queues
        .iter()
        .filter(|queue| queue.counted)
        .map(|queue| QueueStats {
            name: queue.name.clone(),
            active: queue.active,
            scheduled: queue.scheduled,
            inflight: queue
                .workers
                .iter()
                .map(|worker| worker.inflight.len() as u64)
                .sum(),
            done: queue.done,
            dead: queue.dead,
            workers: queue.workers.len() as u64,
            workers_alive: queue
                .workers
                .iter()
                .filter(|worker| redis_api::heartbeat_is_fresh(Some(worker.score), now, window))
                .count() as u64,
        })
        .collect();
    let supervisors: Vec<LoadedSup> = supervisors
        .into_iter()
        .filter(|sup| {
            host_selected(&sup.host, host)
                && redis_api::heartbeat_is_fresh(Some(sup.score), now, window)
        })
        .collect();
    let live: BTreeSet<(String, u32)> = supervisors
        .iter()
        .map(|sup| (sup.host.clone(), sup.pid))
        .collect();
    let mut procs: BTreeMap<(String, u32), ProcBuild> = BTreeMap::new();
    for queue in &queues {
        for worker in &queue.workers {
            let Some(parsed) = parse_worker_name(&worker.name) else {
                continue;
            };
            if !host_selected(&parsed.host, host) {
                continue;
            }
            let pick = WorkerPick {
                name: worker.name.clone(),
                score: worker.score,
                meta: worker.meta.clone(),
                inflight: worker.inflight.clone(),
            };
            procs
                .entry((parsed.host.clone(), parsed.pid))
                .or_insert_with(|| ProcBuild::new(parsed.host, parsed.pid))
                .push_worker(queue.name.clone(), pick);
        }
    }
    for sup in &supervisors {
        for child in child_records(&sup.fields) {
            if child.pid == 0 {
                let matched = procs.iter_mut().find(|((host_name, _), proc)| {
                    host_name == &sup.host
                        && proc.child.is_none()
                        && meta_slot(proc) == Some(child.slot)
                        && meta_ppid(proc) == Some(sup.pid)
                });
                if let Some((_, proc)) = matched {
                    attach_child(proc, child, &sup.host, sup.pid);
                    continue;
                }
            }
            let key = (sup.host.clone(), child.pid);
            if let Some(proc) = procs.get_mut(&key) {
                if proc.child.is_none() {
                    attach_child(proc, child, &sup.host, sup.pid);
                }
            } else {
                procs.insert(
                    key,
                    ProcBuild::from_child(&sup.host, child.pid, sup.pid, child),
                );
            }
        }
    }
    let mut by_supervisor: BTreeMap<(String, u32), Vec<ProcessInfo>> = BTreeMap::new();
    let mut orphans = Vec::new();
    for proc in procs.into_values() {
        let info = finish_process(&proc, now, window, held);
        match place(&proc, &live) {
            Some((host_name, pid)) => by_supervisor
                .entry((host_name, pid))
                .or_default()
                .push(info),
            None => orphans.push(info),
        }
    }
    let mut supervisors = supervisors
        .into_iter()
        .map(|sup| {
            let mut children = by_supervisor
                .remove(&(sup.host.clone(), sup.pid))
                .unwrap_or_default();
            sort_processes(&mut children);
            supervisor_info(&sup, children)
        })
        .collect::<Vec<_>>();
    supervisors.sort_by(|left, right| left.host.cmp(&right.host).then(left.pid.cmp(&right.pid)));
    sort_processes(&mut orphans);
    Snapshot {
        taken_at: now,
        queues: queue_stats,
        supervisors,
        orphans,
    }
}

struct WorkerPick {
    name: String,
    score: f64,
    meta: BTreeMap<String, String>,
    inflight: Vec<String>,
}

struct ProcBuild {
    host: String,
    pid: u32,
    ppid_override: Option<u32>,
    by_queue: BTreeMap<String, WorkerPick>,
    child: Option<ChildRecord>,
    row_owner: Option<(String, u32)>,
}

impl ProcBuild {
    fn new(host: String, pid: u32) -> Self {
        Self {
            host,
            pid,
            ppid_override: None,
            by_queue: BTreeMap::new(),
            child: None,
            row_owner: None,
        }
    }

    fn from_child(host: &str, pid: u32, sup_pid: u32, child: ChildRecord) -> Self {
        Self {
            host: host.to_owned(),
            pid,
            ppid_override: Some(sup_pid),
            by_queue: BTreeMap::new(),
            child: Some(child),
            row_owner: Some((host.to_owned(), sup_pid)),
        }
    }

    fn push_worker(&mut self, queue: String, mut pick: WorkerPick) {
        if let Some(existing) = self.by_queue.get_mut(&queue) {
            if prefer_worker(&pick, existing) {
                union_ids(&mut pick.inflight, &existing.inflight);
                *existing = pick;
            } else {
                union_ids(&mut existing.inflight, &pick.inflight);
            }
        } else {
            self.by_queue.insert(queue, pick);
        }
    }
}

fn prefer_worker(candidate: &WorkerPick, existing: &WorkerPick) -> bool {
    match candidate.score.partial_cmp(&existing.score) {
        Some(std::cmp::Ordering::Greater) => return true,
        Some(std::cmp::Ordering::Less) => return false,
        _ => {}
    }
    match (candidate.meta.is_empty(), existing.meta.is_empty()) {
        (false, true) => return true,
        (true, false) => return false,
        _ => {}
    }
    candidate.name < existing.name
}

fn union_ids(into: &mut Vec<String>, extra: &[String]) {
    for id in extra {
        if !into.iter().any(|have| have == id) {
            into.push(id.clone());
        }
    }
}

fn attach_child(proc: &mut ProcBuild, child: ChildRecord, host: &str, sup_pid: u32) {
    if proc.row_owner.is_none() {
        proc.row_owner = Some((host.to_owned(), sup_pid));
    }
    if proc.ppid_override.is_none() {
        proc.ppid_override = Some(sup_pid);
    }
    proc.child = Some(child);
}

fn meta_ppid(proc: &ProcBuild) -> Option<u32> {
    proc.by_queue
        .values()
        .find_map(|pick| pick.meta.get("ppid")?.parse().ok())
}

fn meta_slot(proc: &ProcBuild) -> Option<u32> {
    proc.by_queue
        .values()
        .find_map(|pick| pick.meta.get("slot")?.parse().ok())
}

fn ppid_of(proc: &ProcBuild) -> Option<u32> {
    meta_ppid(proc).or(proc.ppid_override)
}

fn place(proc: &ProcBuild, live: &BTreeSet<(String, u32)>) -> Option<(String, u32)> {
    if let Some(ppid) = ppid_of(proc) {
        let key = (proc.host.clone(), ppid);
        if live.contains(&key) {
            return Some(key);
        }
    }
    if let Some((host, pid)) = &proc.row_owner {
        let key = (host.clone(), *pid);
        if live.contains(&key) {
            return Some(key);
        }
    }
    None
}

fn finish_process(
    proc: &ProcBuild,
    now: u64,
    window: u64,
    held: &BTreeMap<(String, String), HeldTask>,
) -> ProcessInfo {
    let tasks_done = tasks_done_of(proc);
    let max_tasks = max_tasks(proc);
    let alive = proc
        .by_queue
        .values()
        .any(|pick| redis_api::heartbeat_is_fresh(Some(pick.score), now, window));
    let (state, reason) = process_state(proc, tasks_done, max_tasks, alive);
    let started_meta = proc
        .by_queue
        .values()
        .filter_map(|pick| map_u64(&pick.meta, "started_at"))
        .min();
    let started_at = proc
        .child
        .as_ref()
        .map(|child| child.started_at)
        .filter(|started| *started != 0)
        .or(started_meta)
        .unwrap_or(0);
    let mut workers: Vec<String> = proc
        .by_queue
        .values()
        .map(|pick| pick.name.clone())
        .collect();
    workers.sort();
    let mut tasks = Vec::new();
    for (queue, pick) in &proc.by_queue {
        for task_id in &pick.inflight {
            tasks.push(held_or_default(held, queue, task_id));
        }
    }
    tasks.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then(left.queue.cmp(&right.queue))
    });
    tasks.dedup_by(|left, right| left.queue == right.queue && left.task_id == right.task_id);
    ProcessInfo {
        host: proc.host.clone(),
        pid: proc.pid,
        ppid: ppid_of(proc),
        slot: meta_slot(proc).or(proc.child.as_ref().map(|child| child.slot)),
        state,
        reason,
        exit: proc.child.as_ref().and_then(|child| child.exit),
        rss: proc.child.as_ref().and_then(|child| child.rss),
        started_at,
        ended_at: proc.child.as_ref().and_then(|child| child.ended_at),
        tasks_done,
        max_tasks,
        workers,
        last_seen: proc
            .by_queue
            .values()
            .map(|pick| score_u64(pick.score))
            .max(),
        alive,
        tasks,
    }
}

fn process_state(
    proc: &ProcBuild,
    tasks_done: Option<u64>,
    max_tasks: Option<u64>,
    alive: bool,
) -> (String, Option<String>) {
    if let Some(child) = &proc.child {
        if child.state == ChildState::Running.as_str() {
            if let (Some(done), Some(max)) = (tasks_done, max_tasks) {
                if max > 0 && done >= max {
                    return (
                        ChildState::Draining.as_str().to_owned(),
                        Some(ChildReason::MaxTasks.as_str().to_owned()),
                    );
                }
            }
        }
        return (child.state.clone(), child.reason.clone());
    }
    if !proc.by_queue.values().any(|pick| !pick.meta.is_empty()) {
        return (ChildState::Exited.as_str().to_owned(), None);
    }
    if alive {
        return (ChildState::Running.as_str().to_owned(), None);
    }
    (ChildState::Killed.as_str().to_owned(), None)
}

/// Each queue hash stores the same process counter. Take the newest write, not the sum.
///
/// The supervisor child row keeps the last count after an orderly exit deletes the hash.
fn tasks_done_of(proc: &ProcBuild) -> Option<u64> {
    max_u64(
        proc.by_queue
            .values()
            .filter_map(|pick| map_u64(&pick.meta, "tasks_done"))
            .max(),
        proc.child.as_ref().and_then(|child| child.tasks_done),
    )
}

fn max_tasks(proc: &ProcBuild) -> Option<u64> {
    max_u64(
        proc.by_queue
            .values()
            .filter_map(|pick| map_u64(&pick.meta, "max_tasks"))
            .max(),
        proc.child.as_ref().and_then(|child| child.max_tasks),
    )
}

fn max_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn restore_child_progress(
    existing: &BTreeMap<String, String>,
    child: &mut ChildRecord,
    supervisor_max: u64,
) {
    if let Some(remembered) = remembered_tasks_done(existing, child.pid) {
        child.tasks_done = max_u64(child.tasks_done, Some(remembered));
    }
    if child.max_tasks.is_none() {
        let previous = previous_child(existing, child.pid).and_then(|row| row.max_tasks);
        child.max_tasks = previous.or((supervisor_max > 0).then_some(supervisor_max));
    }
}

fn remembered_tasks_done(existing: &BTreeMap<String, String>, pid: u32) -> Option<u64> {
    let side = existing
        .get(&format!("{TASKS_FIELD}{pid}"))
        .and_then(|text| parse_u64_loose(text));
    let prior = previous_child(existing, pid).and_then(|row| row.tasks_done);
    max_u64(side, prior)
}

fn previous_child(existing: &BTreeMap<String, String>, pid: u32) -> Option<ChildRecord> {
    let text = existing.get(&format!("{CHILD_FIELD}{pid}"))?;
    serde_json::from_str(text).ok()
}

fn sort_processes(processes: &mut [ProcessInfo]) {
    processes.sort_by(|left, right| {
        right
            .ended_at
            .is_none()
            .cmp(&left.ended_at.is_none())
            .then(
                left.slot
                    .unwrap_or(u32::MAX)
                    .cmp(&right.slot.unwrap_or(u32::MAX)),
            )
            .then(left.pid.cmp(&right.pid))
            .then(left.host.cmp(&right.host))
    });
}

fn supervisor_info(sup: &LoadedSup, children: Vec<ProcessInfo>) -> SupervisorInfo {
    let fields = &sup.fields;
    SupervisorInfo {
        host: sup.host.clone(),
        pid: sup.pid,
        started_at: map_u64(fields, "started_at").unwrap_or(0),
        last_seen: score_u64(sup.score),
        version: fields.get("version").cloned().unwrap_or_default(),
        queues: split_queues(fields.get("queues").map(String::as_str).unwrap_or("")),
        processes: fields
            .get("processes")
            .and_then(|text| text.parse().ok())
            .unwrap_or(0),
        concurrency: map_u64(fields, "concurrency").unwrap_or(0),
        max_tasks: map_u64(fields, "max_tasks").unwrap_or(0),
        child_soft: map_u64_opt(fields, "child_soft"),
        child_hard: map_u64_opt(fields, "child_hard"),
        cgroup_current: map_u64_opt(fields, "cgroup_current"),
        cgroup_max: map_u64_opt(fields, "cgroup_max"),
        recycled: map_u64(fields, "recycled").unwrap_or(0),
        drains: map_u64(fields, "drains").unwrap_or(0),
        kills: map_u64(fields, "kills").unwrap_or(0),
        restarts: map_u64(fields, "restarts").unwrap_or(0),
        children,
    }
}

fn child_records(fields: &BTreeMap<String, String>) -> Vec<ChildRecord> {
    let mut records = Vec::new();
    for (name, value) in fields {
        let Some(pid_text) = name.strip_prefix(CHILD_FIELD) else {
            continue;
        };
        let Ok(pid) = pid_text.parse::<u32>() else {
            continue;
        };
        let Ok(mut record) = serde_json::from_str::<ChildRecord>(value) else {
            continue;
        };
        record.pid = pid;
        records.push(record);
    }
    records
}

fn held_or_default(
    held: &BTreeMap<(String, String), HeldTask>,
    queue: &str,
    task_id: &str,
) -> HeldTask {
    held.get(&(queue.to_owned(), task_id.to_owned()))
        .cloned()
        .unwrap_or_else(|| HeldTask {
            queue: queue.to_owned(),
            task_id: task_id.to_owned(),
            message_id: String::new(),
            actor_name: String::new(),
            kwargs: serde_json::json!({}),
            retries: 0,
            max_retries: 0,
            time_limit_ms: 0,
            locked_at: None,
            started_at: None,
            deliveries: 0,
        })
}

pub(crate) fn stored_job(payload: &str) -> Option<Job> {
    let payload = payload.trim();
    if payload.is_empty() {
        return None;
    }
    let text = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(serde_json::Value::String(inner)) => inner,
        _ => payload.to_owned(),
    };
    serde_json::from_str(&text).ok()
}

fn held_task_from_payload(
    queue: &str,
    task_id: &str,
    payload: Option<&str>,
    locked_at: Option<&str>,
    started_at: Option<&str>,
    deliveries: u64,
) -> HeldTask {
    let mut task = HeldTask {
        queue: queue.to_owned(),
        task_id: task_id.to_owned(),
        message_id: String::new(),
        actor_name: String::new(),
        kwargs: serde_json::json!({}),
        retries: 0,
        max_retries: 0,
        time_limit_ms: 0,
        locked_at: locked_at.and_then(parse_u64_loose),
        started_at: started_at.and_then(parse_u64_loose),
        deliveries,
    };
    let Some(job) = payload.and_then(stored_job) else {
        return task;
    };
    task.message_id = job.message_id;
    task.actor_name = job.actor_name;
    if job.kwargs.is_object() {
        task.kwargs = job.kwargs;
    }
    task.retries = job.retries;
    task.max_retries = job.max_retries;
    task.time_limit_ms = job.time_limit_ms;
    task
}

struct ParsedName {
    host: String,
    pid: u32,
}

/// `{prefix}.{queue}.{host...}.{pid}.{suffix}`. The host may contain dots.
fn parse_worker_name(name: &str) -> Option<ParsedName> {
    redis_api::queue_of_worker_name(name)?;
    let fields: Vec<&str> = name.split('.').collect();
    if fields.len() < 5 {
        return None;
    }
    let pid = fields[fields.len() - 2].parse::<u32>().ok()?;
    let host = fields[2..fields.len() - 2].join(".");
    if host.is_empty() {
        return None;
    }
    Some(ParsedName { host, pid })
}

fn parse_supervisor_member(member: &str) -> Option<(String, u32)> {
    let (host, pid) = member.rsplit_once('.')?;
    if host.is_empty() {
        return None;
    }
    let pid = pid.parse::<u32>().ok()?;
    Some((host.to_owned(), pid))
}

fn host_selected(host: &str, filter: Option<&str>) -> bool {
    filter.map(|want| want == host).unwrap_or(true)
}

fn worker_metadata(config: &QueueConfig, name: &str) -> String {
    config.worker_metadata_key(&WorkerContext::new(name))
}

fn worker_inflight(config: &QueueConfig, name: &str) -> String {
    config.inflight_set_for(&WorkerContext::new(name))
}

fn config_for<'a>(prepared: &'a [(String, QueueConfig)], queue: &str) -> Option<&'a QueueConfig> {
    prepared
        .iter()
        .find(|(name, _)| name == queue)
        .map(|(_, config)| config)
}

fn opt_u64(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn split_queues(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|queue| !queue.is_empty())
        .map(str::to_owned)
        .collect()
}

fn map_u64(map: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    map.get(key).and_then(|text| parse_u64_loose(text))
}

fn map_u64_opt(map: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    let text = map.get(key)?;
    parse_u64_loose(text)
}

fn parse_u64_loose(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(value) = text.parse::<u64>() {
        return Some(value);
    }
    let value = text.parse::<f64>().ok()?;
    if value.is_finite() && value >= 0.0 {
        Some(value.round() as u64)
    } else {
        None
    }
}

fn score_u64(score: f64) -> u64 {
    if score.is_finite() && score >= 0.0 {
        score.round() as u64
    } else {
        0
    }
}

async fn redis_now(conn: &mut ConnectionManager) -> Result<u64> {
    let value: redis::Value = redis::cmd("TIME")
        .query_async(conn)
        .await
        .context("failed to read Redis time")?;
    redis_time(&value)
}

async fn query_values(
    conn: &mut ConnectionManager,
    pipe: &redis::Pipeline,
    expected: usize,
    context: &str,
) -> Result<Vec<redis::Value>> {
    if expected == 0 {
        return Ok(Vec::new());
    }
    let values: Vec<redis::Value> = pipe.query_async(conn).await.context(context.to_owned())?;
    if values.len() != expected {
        bail!(
            "{context}: Redis returned {} replies, expected {expected}",
            values.len()
        );
    }
    Ok(values)
}

fn redis_time(value: &redis::Value) -> Result<u64> {
    let redis::Value::Array(items) = value else {
        bail!("Redis TIME returned no seconds");
    };
    let Some(seconds) = items.first() else {
        bail!("Redis TIME returned no seconds");
    };
    value_string(seconds)
        .and_then(|text| text.parse::<u64>().ok())
        .context("Redis TIME seconds were not an integer")
}

fn hash_map(value: &redis::Value) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    match value {
        redis::Value::Array(items) => {
            let mut index = 0;
            while index + 1 < items.len() {
                if let (Some(key), Some(field)) =
                    (value_string(&items[index]), value_string(&items[index + 1]))
                {
                    map.insert(key, field);
                }
                index += 2;
            }
        }
        redis::Value::Map(pairs) => {
            for (key, field) in pairs {
                if let (Some(key), Some(field)) = (value_string(key), value_string(field)) {
                    map.insert(key, field);
                }
            }
        }
        redis::Value::Nil => {}
        _ => {}
    }
    map
}

fn string_list(value: &redis::Value) -> Vec<String> {
    match value {
        redis::Value::Array(items) | redis::Value::Set(items) => {
            items.iter().filter_map(value_string).collect()
        }
        redis::Value::Nil => Vec::new(),
        _ => Vec::new(),
    }
}

fn scored_members(value: &redis::Value) -> Vec<(String, f64)> {
    let redis::Value::Array(items) = value else {
        return Vec::new();
    };
    let mut members = Vec::new();
    let mut index = 0;
    while index + 1 < items.len() {
        if let Some(member) = value_string(&items[index]) {
            if let Some(score) = value_f64(&items[index + 1]) {
                members.push((member, score));
            }
        }
        index += 2;
    }
    members
}

fn nth_string(value: &redis::Value, index: usize) -> Option<String> {
    match value {
        redis::Value::Array(items) => items.get(index).and_then(value_string),
        other if index == 0 => value_string(other),
        _ => None,
    }
}

fn value_string(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::Nil => None,
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(text) => Some(text.clone()),
        redis::Value::VerbatimString { text, .. } => Some(text.clone()),
        redis::Value::Int(number) => Some(number.to_string()),
        redis::Value::Double(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_u64(value: &redis::Value) -> u64 {
    match value {
        redis::Value::Int(number) if *number > 0 => *number as u64,
        redis::Value::Int(_) => 0,
        redis::Value::Double(number) if *number > 0.0 => *number as u64,
        other => value_string(other)
            .and_then(|text| parse_u64_loose(&text))
            .unwrap_or(0),
    }
}

fn value_f64(value: &redis::Value) -> Option<f64> {
    match value {
        redis::Value::Double(number) => Some(*number),
        redis::Value::Int(number) => Some(*number as f64),
        other => value_string(other)?.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_class(
        signal: SentSignal,
        exit_code: Option<i32>,
        term_signal: Option<i32>,
        state: ChildState,
        reason: Option<ChildReason>,
    ) {
        let (got_state, got_reason) = classify_child(signal, exit_code, term_signal);
        assert_eq!(got_state, state, "{signal:?} {exit_code:?} {term_signal:?}");
        assert_eq!(
            got_reason, reason,
            "{signal:?} {exit_code:?} {term_signal:?}"
        );
        assert_eq!(got_state.as_str(), state.as_str());
        assert_eq!(
            got_reason.map(ChildReason::as_str),
            reason.map(ChildReason::as_str)
        );
    }

    #[test]
    fn classify_child_covers_every_exit_path() {
        assert_class(SentSignal::None, None, None, ChildState::Running, None);
        assert_class(
            SentSignal::Usr1(DrainCause::MemorySoft),
            None,
            None,
            ChildState::Draining,
            Some(ChildReason::MemorySoft),
        );
        assert_class(
            SentSignal::Usr1(DrainCause::MemoryRatio),
            None,
            None,
            ChildState::Draining,
            Some(ChildReason::MemoryRatio),
        );
        assert_class(
            SentSignal::Usr1(DrainCause::Shutdown),
            None,
            None,
            ChildState::Draining,
            Some(ChildReason::Shutdown),
        );
        assert_class(
            SentSignal::Usr1(DrainCause::MemorySoft),
            Some(0),
            None,
            ChildState::Exited,
            Some(ChildReason::MemorySoft),
        );
        assert_class(
            SentSignal::Usr1(DrainCause::MemoryRatio),
            Some(0),
            None,
            ChildState::Exited,
            Some(ChildReason::MemoryRatio),
        );
        assert_class(
            SentSignal::Usr1(DrainCause::Shutdown),
            Some(0),
            None,
            ChildState::Exited,
            Some(ChildReason::Shutdown),
        );
        assert_class(
            SentSignal::None,
            Some(0),
            None,
            ChildState::Exited,
            Some(ChildReason::MaxTasks),
        );
        assert_class(
            SentSignal::Kill(KillCause::MemoryHard),
            None,
            None,
            ChildState::Killed,
            Some(ChildReason::MemoryHard),
        );
        assert_class(
            SentSignal::Kill(KillCause::MemoryRatio),
            None,
            None,
            ChildState::Killed,
            Some(ChildReason::MemoryRatio),
        );
        assert_class(
            SentSignal::Kill(KillCause::MemoryHard),
            None,
            Some(SIGKILL),
            ChildState::Killed,
            Some(ChildReason::MemoryHard),
        );
        assert_class(
            SentSignal::Kill(KillCause::MemoryRatio),
            None,
            Some(SIGKILL),
            ChildState::Killed,
            Some(ChildReason::MemoryRatio),
        );
        assert_class(
            SentSignal::None,
            Some(2),
            None,
            ChildState::Killed,
            Some(ChildReason::PingBudget),
        );
        assert_class(
            SentSignal::None,
            None,
            Some(SIGKILL),
            ChildState::Killed,
            Some(ChildReason::Sigkill),
        );
        assert_class(
            SentSignal::Usr1(DrainCause::MemorySoft),
            None,
            Some(SIGKILL),
            ChildState::Killed,
            Some(ChildReason::Sigkill),
        );
        assert_class(
            SentSignal::None,
            Some(1),
            None,
            ChildState::Killed,
            Some(ChildReason::Crash),
        );
        assert_class(
            SentSignal::None,
            None,
            Some(15),
            ChildState::Killed,
            Some(ChildReason::Crash),
        );
    }

    #[test]
    fn counters_follow_recycle_drain_and_kill() {
        let mut recycled = ChildTrack::start(1);
        let mut counters = Counters::default();
        recycled.apply_exit(Some(0), None, 10, &mut counters);
        assert_eq!(counters.recycled, 1);
        assert_eq!(counters.restarts, 0);
        assert_eq!(recycled.record().reason.as_deref(), Some("max_tasks"));

        let mut soft = ChildTrack::start(1);
        let mut counters = Counters::default();
        soft.apply_signal(SentSignal::Usr1(DrainCause::MemorySoft), &mut counters);
        assert_eq!(counters.drains, 1);
        assert_eq!(soft.record().state, "draining");
        assert_eq!(soft.record().reason.as_deref(), Some("memory_soft"));
        soft.apply_signal(SentSignal::Usr1(DrainCause::Shutdown), &mut counters);
        assert_eq!(counters.drains, 1);
        assert!(matches!(
            soft.signal,
            SentSignal::Usr1(DrainCause::MemorySoft)
        ));

        let mut shutdown = ChildTrack::start(1);
        let mut counters = Counters::default();
        shutdown.apply_signal(SentSignal::Usr1(DrainCause::Shutdown), &mut counters);
        assert_eq!(counters.drains, 0);
        assert_eq!(shutdown.record().reason.as_deref(), Some("shutdown"));

        for cause in [KillCause::MemoryHard, KillCause::MemoryRatio] {
            let mut track = ChildTrack::start(1);
            let mut counters = Counters::default();
            track.apply_signal(SentSignal::Kill(cause), &mut counters);
            assert_eq!(counters.kills, 1, "{cause:?}");
            assert_eq!(counters.restarts, 1, "{cause:?}");
            assert_eq!(track.record().state, "killed");
            assert!(track.ended_at.is_none());
            track.apply_exit(None, Some(SIGKILL), 20, &mut counters);
            assert_eq!(counters.kills, 1, "{cause:?}");
            assert_eq!(counters.restarts, 1, "{cause:?}");
            assert_eq!(track.record().state, "killed");
            assert_eq!(track.record().exit, Some(SIGKILL));
        }

        for (code, term, reason) in [
            (Some(2), None, "ping_budget"),
            (None, Some(SIGKILL), "sigkill"),
            (Some(1), None, "crash"),
            (None, Some(15), "crash"),
        ] {
            let mut track = ChildTrack::start(1);
            let mut counters = Counters::default();
            track.apply_exit(code, term, 5, &mut counters);
            assert_eq!(counters.restarts, 1, "{reason}");
            assert_eq!(counters.kills, 0, "{reason}");
            assert_eq!(counters.recycled, 0, "{reason}");
            assert_eq!(track.record().reason.as_deref(), Some(reason));
        }

        let mut drained = ChildTrack::start(1);
        let mut counters = Counters::default();
        drained.apply_signal(SentSignal::Usr1(DrainCause::MemorySoft), &mut counters);
        drained.apply_exit(Some(0), None, 30, &mut counters);
        assert_eq!(
            (
                counters.drains,
                counters.recycled,
                counters.kills,
                counters.restarts
            ),
            (1, 1, 0, 0)
        );
        assert_eq!(drained.record().state, "exited");

        let mut both = ChildTrack::start(1);
        let mut counters = Counters::default();
        both.apply_signal(SentSignal::Usr1(DrainCause::MemoryRatio), &mut counters);
        both.apply_signal(SentSignal::Kill(KillCause::MemoryHard), &mut counters);
        assert_eq!(
            (
                counters.drains,
                counters.kills,
                counters.restarts,
                counters.recycled
            ),
            (1, 1, 1, 0)
        );
        both.apply_signal(SentSignal::Kill(KillCause::MemoryRatio), &mut counters);
        assert_eq!(counters.kills, 1);
        assert_eq!(both.record().reason.as_deref(), Some("memory_hard"));

        let mut once = ChildTrack::start(1);
        let mut counters = Counters::default();
        once.apply_exit(Some(0), None, 10, &mut counters);
        once.apply_exit(Some(1), None, 11, &mut counters);
        assert_eq!(counters.recycled, 1);
        assert_eq!(counters.restarts, 0);
        assert_eq!(once.ended_at, Some(10));
        assert_eq!(once.exit_code, Some(0));
    }

    #[test]
    fn prune_finished_drops_a_row_at_exactly_two_windows() {
        let window = 60u64;
        let retain = window * 2;
        let now = 10_000u64;
        assert!(finished_row_expired(Some(now - retain), now, retain));
        assert!(!finished_row_expired(Some(now - retain + 1), now, retain));
        assert!(!finished_row_expired(None, now, retain));
        let mut children = vec![
            row(1, Some(now - retain)),
            row(2, Some(now - retain + 1)),
            row(3, None),
        ];
        prune_finished(&mut children, now, retain);
        let slots: Vec<u32> = children.iter().map(|child| child.slot).collect();
        assert_eq!(slots, vec![2, 3]);
        assert!(children.iter().any(|child| child.ended_at.is_none()));
    }

    fn row(slot: u32, ended_at: Option<u64>) -> ChildRecord {
        ChildRecord {
            pid: slot,
            slot,
            state: if ended_at.is_none() {
                "running".to_owned()
            } else {
                "exited".to_owned()
            },
            reason: None,
            rss: None,
            started_at: 1,
            ended_at,
            exit: None,
            tasks_done: None,
            max_tasks: None,
        }
    }

    #[test]
    fn supervisor_keys_and_ttl_use_the_heartbeat_window() {
        assert_eq!(SUPERVISORS_KEY, "simple_q:supervisors");
        assert_eq!(supervisor_member("info-host", 12), "info-host.12");
        assert_eq!(
            supervisor_key("info-host.12"),
            "simple_q:supervisor:info-host.12"
        );
        assert_eq!(supervisor_member("info.host", 12), "info.host.12");
        let mut settings = Config::defaults();
        assert_eq!(supervisor_ttl_secs(&settings), 15 * 4 * 2);
        settings.heartbeat_secs = 2;
        settings.missed_heartbeats = 3;
        assert_eq!(supervisor_ttl_secs(&settings), 12);
        settings.heartbeat_secs = 0;
        settings.missed_heartbeats = 0;
        assert_eq!(supervisor_ttl_secs(&settings), 1);
    }

    #[test]
    fn worker_and_supervisor_names_keep_a_dotted_host() {
        let parsed = parse_worker_name("agent-worker.holdq.host.example.com.7.abcd1234").unwrap();
        assert_eq!(parsed.host, "host.example.com");
        assert_eq!(parsed.pid, 7);
        assert!(parse_worker_name("agent-worker.holdq.host.7").is_none());
        assert_eq!(
            parse_supervisor_member("host.example.com.42").unwrap(),
            ("host.example.com".to_owned(), 42)
        );
        assert!(parse_supervisor_member(".7").is_none());
        assert!(parse_supervisor_member("nopid").is_none());
    }

    #[test]
    fn child_record_json_omits_absent_fields_and_the_pid() {
        let record = ChildTrack::start(3).record();
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json["slot"], 3);
        assert_eq!(json["state"], "running");
        assert_eq!(json["started_at"], 0);
        assert!(json.get("reason").is_none());
        assert!(json.get("rss").is_none());
        assert!(json.get("ended_at").is_none());
        assert!(json.get("exit").is_none());
        assert!(json.get("pid").is_none());
        assert!(json.get("tasks_done").is_none());
        assert!(json.get("max_tasks").is_none());
        assert_eq!(record.exit, None);
    }

    #[test]
    fn an_exited_row_keeps_the_task_count_without_metadata() {
        let mut child = running_child(12, 1);
        child.state = "exited".to_owned();
        child.reason = Some("max_tasks".to_owned());
        child.tasks_done = Some(500);
        child.max_tasks = Some(500);
        child.ended_at = Some(80);
        let proc = ProcBuild::from_child("host", 12, 1, child);
        assert_eq!(tasks_done_of(&proc), Some(500));
        assert_eq!(max_tasks(&proc), Some(500));

        let mut existing = BTreeMap::new();
        existing.insert("tasks:12".to_owned(), "500".to_owned());
        let mut fresh = row(12, Some(80));
        restore_child_progress(&existing, &mut fresh, 500);
        assert_eq!(fresh.tasks_done, Some(500));
        assert_eq!(fresh.max_tasks, Some(500));
        existing.remove("tasks:12");
        existing.insert(
            "child:12".to_owned(),
            serde_json::to_string(&fresh).unwrap(),
        );
        let mut again = row(12, Some(80));
        restore_child_progress(&existing, &mut again, 500);
        assert_eq!(again.tasks_done, Some(500));
        assert_eq!(again.max_tasks, Some(500));
    }

    #[test]
    fn a_missing_payload_still_yields_the_task_id() {
        let missing = held_task_from_payload("holdq", "task-1", None, None, None, 0);
        assert_eq!(missing.task_id, "task-1");
        assert_eq!(missing.actor_name, "");
        assert_eq!(missing.kwargs, serde_json::json!({}));
        assert!(missing.locked_at.is_none());

        let job = Job {
            version: crate::MESSAGE_VERSION,
            message_id: "m-1".to_owned(),
            actor_name: "recalc".to_owned(),
            queue: "holdq".to_owned(),
            kwargs: serde_json::json!({"project": "42"}),
            headers: Default::default(),
            retries: 1,
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
        let payload = serde_json::to_string(&job).unwrap();
        let task = held_task_from_payload(
            "holdq",
            "ulid-1",
            Some(&payload),
            Some("1700000000"),
            Some("1699999000"),
            4,
        );
        assert_eq!(task.task_id, "ulid-1");
        assert_eq!(task.message_id, "m-1");
        assert_eq!(task.actor_name, "recalc");
        assert_eq!(task.kwargs, serde_json::json!({"project": "42"}));
        assert_eq!(task.retries, 1);
        assert_eq!(task.locked_at, Some(1_700_000_000));
        assert_eq!(task.started_at, Some(1_699_999_000));
        assert_eq!(task.deliveries, 4);
    }

    fn worker(name: &str, score: f64, meta: &[(&str, &str)], inflight: &[&str]) -> LoadedWorker {
        LoadedWorker {
            name: name.to_owned(),
            score,
            meta: meta
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            inflight: inflight.iter().map(|id| (*id).to_owned()).collect(),
        }
    }

    fn running_child(pid: u32, slot: u32) -> ChildRecord {
        let mut child = ChildTrack::start(slot).record();
        child.pid = pid;
        child.started_at = 50;
        child
    }

    #[test]
    fn the_tree_joins_children_orphans_and_queue_counts() {
        let now = 1_000u64;
        let window = 60u64;
        let host = "host.example.com";
        let worker_name = "agent-worker.holdq.host.example.com.7.abcd1234";
        let mut child = running_child(7, 0);
        child.rss = Some(10);
        let child_json = serde_json::to_string(&child).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("version".to_owned(), "1.0.0-rc1".to_owned());
        fields.insert("queues".to_owned(), "holdq".to_owned());
        fields.insert("processes".to_owned(), "1".to_owned());
        fields.insert("concurrency".to_owned(), "2".to_owned());
        fields.insert("max_tasks".to_owned(), "500".to_owned());
        fields.insert("child_soft".to_owned(), String::new());
        fields.insert("started_at".to_owned(), "40".to_owned());
        fields.insert(format!("child:{pid}", pid = 7), child_json);
        let mut finished = running_child(8, 1);
        finished.state = "exited".to_owned();
        finished.ended_at = Some(900);
        finished.reason = Some("max_tasks".to_owned());
        fields.insert(
            "child:8".to_owned(),
            serde_json::to_string(&finished).unwrap(),
        );
        let queues = vec![QueueDraft {
            name: "holdq".to_owned(),
            active: 1,
            scheduled: 0,
            done: 0,
            dead: 0,
            counted: true,
            workers: vec![
                worker(
                    worker_name,
                    now as f64,
                    &[
                        ("ppid", "3"),
                        ("slot", "0"),
                        ("started_at", "51"),
                        ("max_tasks", "500"),
                        ("tasks_done", "2"),
                    ],
                    &["b-task", "a-task"],
                ),
                worker(
                    "agent-worker.holdq.other-host.9.abcd1234",
                    now as f64,
                    &[("ppid", "1")],
                    &["hidden-1", "hidden-2"],
                ),
            ],
        }];
        let held = BTreeMap::from([
            (
                ("holdq".to_owned(), "a-task".to_owned()),
                HeldTask {
                    queue: "holdq".to_owned(),
                    task_id: "a-task".to_owned(),
                    message_id: "m-a".to_owned(),
                    actor_name: "recalc".to_owned(),
                    kwargs: serde_json::json!({"project": "42"}),
                    retries: 1,
                    max_retries: 2,
                    time_limit_ms: 30,
                    locked_at: Some(90),
                    started_at: Some(80),
                    deliveries: 3,
                },
            ),
            (
                ("holdq".to_owned(), "b-task".to_owned()),
                held_task_from_payload("holdq", "b-task", None, None, None, 0),
            ),
        ]);
        let snap = build_snapshot(
            now,
            window,
            Some(host),
            queues,
            vec![LoadedSup {
                host: host.to_owned(),
                pid: 3,
                score: now as f64,
                fields,
            }],
            &held,
        );
        assert_eq!(snap.taken_at, now);
        assert_eq!(snap.queues[0].inflight, 4);
        assert_eq!(snap.queues[0].workers, 2);
        assert_eq!(snap.queues[0].workers_alive, 2);
        assert_eq!(snap.supervisors.len(), 1);
        assert!(snap.supervisors[0].child_soft.is_none());
        assert_eq!(snap.supervisors[0].queues, vec!["holdq".to_owned()]);
        assert_eq!(snap.supervisors[0].children.len(), 2);
        let live = &snap.supervisors[0].children[0];
        assert!(live.ended_at.is_none());
        assert_eq!(live.pid, 7);
        assert_eq!(live.slot, Some(0));
        assert_eq!(live.state, "running");
        assert_eq!(live.ppid, Some(3));
        assert_eq!(live.workers, vec![worker_name.to_owned()]);
        assert_eq!(live.tasks.len(), 2);
        assert_eq!(live.tasks[0].task_id, "a-task");
        assert_eq!(live.tasks[0].actor_name, "recalc");
        assert_eq!(live.tasks[1].task_id, "b-task");
        assert_eq!(live.tasks[1].actor_name, "");
        let done = &snap.supervisors[0].children[1];
        assert_eq!(done.pid, 8);
        assert_eq!(done.state, "exited");
        assert!(done.workers.is_empty());
        assert!(snap.orphans.is_empty());
    }

    #[test]
    fn orphans_follow_the_metadata_hash_and_the_heartbeat() {
        let now = 1_000u64;
        let window = 60u64;
        let host = "info-host";
        let queues = vec![QueueDraft {
            name: "holdq".to_owned(),
            active: 0,
            scheduled: 0,
            done: 0,
            dead: 0,
            counted: true,
            workers: vec![
                worker(
                    "agent-worker.holdq.info-host.11.stale001",
                    (now - window - 1) as f64,
                    &[("ppid", "99"), ("tasks_done", "1")],
                    &[],
                ),
                worker(
                    "agent-worker.holdq.info-host.12.nometa01",
                    now as f64,
                    &[],
                    &[],
                ),
                worker(
                    "agent-worker.holdq.info-host.13.fresh001",
                    (now - window) as f64,
                    &[("ppid", "99")],
                    &[],
                ),
            ],
        }];
        let snap = build_snapshot(
            now,
            window,
            Some(host),
            queues,
            Vec::new(),
            &BTreeMap::new(),
        );
        assert!(snap.supervisors.is_empty());
        assert_eq!(snap.queues[0].workers_alive, 2);
        let states: Vec<(&str, u32, &str, bool)> = snap
            .orphans
            .iter()
            .map(|proc| {
                (
                    proc.state.as_str(),
                    proc.pid,
                    proc.reason.as_deref().unwrap_or(""),
                    proc.alive,
                )
            })
            .collect();
        assert_eq!(
            states,
            vec![
                ("killed", 11, "", false),
                ("exited", 12, "", true),
                ("running", 13, "", true),
            ]
        );
    }

    #[test]
    fn a_running_child_at_max_tasks_is_shown_as_draining() {
        let now = 1_000u64;
        let child = running_child(7, 0);
        let mut fields = BTreeMap::new();
        fields.insert("child:7".to_owned(), serde_json::to_string(&child).unwrap());
        let queues = vec![QueueDraft {
            name: "holdq".to_owned(),
            active: 0,
            scheduled: 0,
            done: 0,
            dead: 0,
            counted: true,
            workers: vec![worker(
                "agent-worker.holdq.info-host.7.abcd1234",
                now as f64,
                &[("ppid", "3"), ("tasks_done", "5"), ("max_tasks", "5")],
                &[],
            )],
        }];
        let snap = build_snapshot(
            now,
            60,
            None,
            queues,
            vec![LoadedSup {
                host: "info-host".to_owned(),
                pid: 3,
                score: now as f64,
                fields,
            }],
            &BTreeMap::new(),
        );
        let shown = &snap.supervisors[0].children[0];
        assert_eq!(shown.state, "draining");
        assert_eq!(shown.reason.as_deref(), Some("max_tasks"));
        assert_eq!(shown.tasks_done, Some(5));
        assert_eq!(shown.max_tasks, Some(5));
    }

    #[test]
    fn an_uncounted_queue_adds_tasks_without_a_counter_row() {
        let now = 1_000u64;
        let quota_worker = "agent-worker.quota.info-host.7.abcd1234";
        let default_worker = "agent-worker.default.info-host.7.abcd1234";
        let queues = vec![
            QueueDraft {
                name: "quota".to_owned(),
                active: 2,
                scheduled: 0,
                done: 0,
                dead: 0,
                counted: true,
                workers: vec![worker(
                    quota_worker,
                    now as f64,
                    &[("ppid", "3")],
                    &["quota-task"],
                )],
            },
            QueueDraft {
                name: "default".to_owned(),
                active: 9,
                scheduled: 4,
                done: 1,
                dead: 1,
                counted: false,
                workers: vec![worker(
                    default_worker,
                    now as f64,
                    &[("ppid", "3")],
                    &["default-task"],
                )],
            },
        ];
        let mut fields = BTreeMap::new();
        fields.insert("queues".to_owned(), "quota,default".to_owned());
        let mut child = running_child(7, 0);
        child.pid = 7;
        fields.insert("child:7".to_owned(), serde_json::to_string(&child).unwrap());
        let held = BTreeMap::from([
            (
                ("quota".to_owned(), "quota-task".to_owned()),
                held_task_from_payload("quota", "quota-task", None, None, None, 0),
            ),
            (
                ("default".to_owned(), "default-task".to_owned()),
                held_task_from_payload("default", "default-task", None, None, None, 0),
            ),
        ]);
        let snap = build_snapshot(
            now,
            60,
            None,
            queues,
            vec![LoadedSup {
                host: "info-host".to_owned(),
                pid: 3,
                score: now as f64,
                fields,
            }],
            &held,
        );
        assert_eq!(snap.queues.len(), 1);
        assert_eq!(snap.queues[0].name, "quota");
        assert_eq!(snap.queues[0].active, 2);
        assert_eq!(snap.queues[0].inflight, 1);
        let tasks: Vec<&str> = snap.supervisors[0].children[0]
            .tasks
            .iter()
            .map(|task| task.task_id.as_str())
            .collect();
        assert_eq!(tasks, vec!["default-task", "quota-task"]);
        assert_eq!(
            snap.supervisors[0].children[0].workers,
            vec![default_worker.to_owned(), quota_worker.to_owned()]
        );
    }
}
