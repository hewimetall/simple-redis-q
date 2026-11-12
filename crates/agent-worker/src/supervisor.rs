//! User side that only forks and reaps children.
//!
//! It does not take messages. Exit code 3 from a child is a bad tracing config
//! and stops the supervisor. A child-memory limit that does not parse, is zero,
//! or has soft greater than or equal to hard is also exit code 3, from this
//! process, before any child starts. Any other non-zero child code waits and
//! starts that slot again. A panic while the slots are draining is a supervisor
//! failure.
//!
//! Each child has optional RSS byte limits. Once a second the supervisor reads
//! RSS. A soft signal skips a slot whose delivery digit is not zero. A hard
//! signal does not read that digit. At the soft limit every free child over it
//! gets one SIGUSR1. At the hard limit every child over it is SIGKILLed.
//! Several children can be signaled in the same second.
//! The container ratio is a backstop: usage is `memory.current` minus
//! `inactive_file`. At the soft ratio one free child that has not yet been
//! drained, the largest RSS, gets SIGUSR1. A busy child is left for a later
//! second. At the hard ratio one child, the largest, is SIGKILLed when its
//! RSS is at least half the usage. The hard path does not read the digit.
//! Otherwise nobody is killed for the ratio and the largest free child that
//! has not been asked to drain is drained instead.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

use simple_q_runtime::admin::{
    ChildTrack, Counters, DrainCause, KillCause, SentSignal, SupervisorRecord, prune_finished,
    publish_supervisor, supervisor_ttl_secs,
};
use simple_q_runtime::config::Config;
use simple_q_runtime::ids::hostname;
use simple_q_runtime::names::validate_queue;
use simple_q_runtime::redis_api::{self, ConnectionManager};
use simple_q_runtime::slots::{reset_slot_gauges, wipe_db_files};

pub async fn run(
    app: String,
    factory: bool,
    queues: Vec<String>,
    processes: u32,
    concurrency: usize,
    max_tasks: u64,
    limits: ChildMemoryLimits,
    memory_soft_ratio: f64,
    memory_hard_ratio: f64,
    redis_url: String,
    settings: Config,
    signals: &mut StopSignals,
) -> Result<()> {
    if processes == 0 {
        anyhow::bail!("--processes must be greater than zero");
    }
    if concurrency == 0 {
        anyhow::bail!("--concurrency must be greater than zero");
    }
    if max_tasks == 0 {
        anyhow::bail!("--max-tasks-per-process must be greater than zero");
    }
    for queue in &queues {
        validate_queue(queue)?;
    }

    // A SIGTERM during the application import is already recorded. Yield so
    // the runtime driver delivers it before any child is forked.
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    if stop_requested(signals).await {
        tracing::info!("stop signal received before workers started");
        return Ok(());
    }

    let prom_dir = prom_dir();
    std::fs::create_dir_all(&prom_dir).context("failed to create the metrics directory")?;
    wipe_db_files(&prom_dir).context("failed to clear the metrics directory")?;
    let memory_metrics = ChildMemoryMetrics::new(prom_dir.clone());
    log_child_memory_limits(&limits);
    warn_if_hard_budget_exceeds_cgroup(processes, limits.hard);
    let metrics = spawn_metrics(&prom_dir)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let live = LivePids::default();
    let roster = Roster::default();
    let mut pressure = Pressure::default();
    let (stop_heartbeat_tx, stop_heartbeat_rx) = watch::channel(false);
    let host = hostname();
    let supervisor_pid = std::process::id();
    let heartbeat = spawn_supervisor_heartbeat(
        redis_url.clone(),
        settings,
        host.clone(),
        supervisor_pid,
        stop_heartbeat_rx,
        queues.clone(),
        processes,
        concurrency as u64,
        max_tasks,
        limits,
        roster.clone(),
    );

    let digits = crate::slot_busy::open(processes)?;
    let mut slots = tokio::task::JoinSet::new();
    for index in 0..processes {
        let app = app.clone();
        let queues = queues.clone();
        let shutdown = shutdown_rx.clone();
        let shutdown_tx = shutdown_tx.clone();
        let prom_dir = prom_dir.clone();
        let live = live.clone();
        let roster = roster.clone();
        let digits = digits.clone();
        slots.spawn(async move {
            supervise_slot(
                index,
                app,
                factory,
                queues,
                concurrency,
                max_tasks,
                shutdown,
                shutdown_tx,
                prom_dir,
                live,
                roster,
                digits,
            )
            .await
        });
    }

    let mut fatal = None;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                apply_memory_pressure(
                &live,
                &mut pressure,
                limits,
                memory_soft_ratio,
                memory_hard_ratio,
                &memory_metrics,
                &roster,
                digits.as_deref(),
            );
            }
            _ = signals.terminate.recv() => {
                tracing::info!("SIGTERM received; draining child processes");
                let _ = shutdown_tx.send(true);
                break;
            }
            _ = signals.interrupt.recv() => {
                tracing::info!("SIGINT received; draining child processes");
                let _ = shutdown_tx.send(true);
                break;
            }
            joined = slots.join_next() => {
                match joined {
                    None => {
                        drop(metrics);
                        retire_supervisor(
                            stop_heartbeat_tx,
                            heartbeat,
                            &redis_url,
                            &host,
                            supervisor_pid,
                        )
                        .await;
                        return Ok(());
                    }
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => {
                        tracing::error!(%error, "stopping supervisor");
                        let _ = shutdown_tx.send(true);
                        fatal = Some(error);
                        break;
                    }
                    Some(Err(error)) => {
                        let _ = shutdown_tx.send(true);
                        fatal = Some(anyhow::anyhow!("supervisor slot panicked: {error}"));
                        break;
                    }
                }
            }
        }
    }

    while let Some(joined) = slots.join_next().await {
        note_shutdown_join(joined, &mut fatal);
    }
    drop(metrics);
    retire_supervisor(
        stop_heartbeat_tx,
        heartbeat,
        &redis_url,
        &host,
        supervisor_pid,
    )
    .await;
    match fatal {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn supervise_slot(
    index: u32,
    app: String,
    factory: bool,
    queues: Vec<String>,
    concurrency: usize,
    max_tasks: u64,
    mut shutdown: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
    prom_dir: std::path::PathBuf,
    live: LivePids,
    roster: Roster,
    digits: Option<std::sync::Arc<crate::slot_busy::SlotDigits>>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        reset_slot_gauges(&prom_dir, index).context("failed to reset slot gauges")?;
        if let Some(digits) = &digits {
            digits.reset(index);
        }
        let mut child = spawn_child(
            &app,
            factory,
            &queues,
            concurrency,
            max_tasks,
            index,
            &prom_dir,
            digits.as_ref().map(|digits| digits.raw_fd()),
        )?;
        let pid = child.id().context("child process has no pid")?;
        tracing::info!(index, pid, "worker process started");
        roster.start(index, pid);

        let status = {
            let _registered = LiveGuard::insert(live.clone(), index, pid);
            tokio::select! {
                changed = shutdown.changed() => {
                    changed.context("shutdown watch closed")?;
                    if request_drain(pid) {
                        roster.signal(pid, SentSignal::Usr1(DrainCause::Shutdown));
                    }
                    if let Ok(status) = child.wait().await {
                        let (code, term) = exit_parts(&status);
                        roster.finish(pid, code, term);
                    }
                    return Ok(());
                }
                status = child.wait() => status,
            }
        };
        let code = status.context("failed to wait for the worker process")?;
        let (exit_code, term_signal) = exit_parts(&code);
        // Shutdown can win the race with this wait. Record the exit before
        // returning, or the roster keeps the child as running.
        roster.finish(pid, exit_code, term_signal);
        if *shutdown.borrow() {
            return Ok(());
        }
        match classify_child_exit(code.success(), code.code()) {
            ChildExit::Recycle => {
                tracing::info!(index, pid, "worker process recycled");
                backoff = Duration::from_secs(1);
            }
            ChildExit::Fatal => {
                tracing::error!(index, pid, "tracing configuration is invalid");
                let _ = shutdown_tx.send(true);
                anyhow::bail!("tracing configuration is invalid");
            }
            ChildExit::Restart => {
                tracing::warn!(index, pid, ?code, "worker process exited");
                if *shutdown.borrow() {
                    return Ok(());
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        changed.context("shutdown watch closed")?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// How the supervisor treats one child exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildExit {
    /// Planned recycle. Start a replacement immediately.
    Recycle,
    /// Crash or signal. Wait out the backoff, then start a replacement.
    Restart,
    /// Tracing configuration was rejected. Do not start another child.
    Fatal,
}

pub fn classify_child_exit(success: bool, code: Option<i32>) -> ChildExit {
    if success {
        return ChildExit::Recycle;
    }
    if code == Some(3) {
        return ChildExit::Fatal;
    }
    ChildExit::Restart
}

fn spawn_child(
    app: &str,
    factory: bool,
    queues: &[String],
    concurrency: usize,
    max_tasks: u64,
    slot: u32,
    prom_dir: &std::path::Path,
    slot_fd: Option<i32>,
) -> Result<Child> {
    let executable = std::env::current_exe().context("failed to resolve the worker executable")?;
    let mut command = Command::new(executable);
    command.arg("worker").arg("--child").arg("--app").arg(app);
    if factory {
        command.arg("--factory");
    }
    command
        .arg("--queues")
        .arg(queues.join(","))
        .arg("--concurrency")
        .arg(concurrency.to_string())
        .arg("--max-tasks-per-process")
        .arg(max_tasks.to_string())
        .env("AGENT_WORKER_CHILD", "1")
        .env("SIMPLE_Q_SLOT", slot.to_string())
        .env("PROMETHEUS_MULTIPROC_DIR", prom_dir)
        .env("prometheus_multiproc_dir", prom_dir)
        .env("DRAMATIQ_PROM_DB", prom_dir)
        .env("dramatiq_prom_db", prom_dir)
        .kill_on_drop(true);
    if let Some(fd) = slot_fd {
        command.env("SIMPLE_Q_SLOT_FD", fd.to_string());
        // SAFETY: the closure runs in the forked child, before exec. It only
        // calls fcntl(F_GETFD) and fcntl(F_SETFD). The parent descriptor stays
        // cloexec, so a spawn on another thread does not inherit it.
        unsafe {
            command.pre_exec(move || crate::slot_busy::clear_cloexec(fd));
        }
    }
    command.spawn().context("failed to spawn a worker process")
}

fn prom_dir() -> std::path::PathBuf {
    std::env::var("DRAMATIQ_PROM_DB")
        .or_else(|_| std::env::var("PROMETHEUS_MULTIPROC_DIR"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("dramatiq-prometheus"))
}

fn spawn_metrics(prom_dir: &std::path::Path) -> Result<Child> {
    let executable = std::env::current_exe().context("failed to resolve the worker executable")?;
    Command::new(executable)
        .arg("metrics")
        .env("PROMETHEUS_MULTIPROC_DIR", prom_dir)
        .env("prometheus_multiproc_dir", prom_dir)
        .env("DRAMATIQ_PROM_DB", prom_dir)
        .env("dramatiq_prom_db", prom_dir)
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn the metrics process")
}

fn note_shutdown_join(
    joined: Result<Result<()>, tokio::task::JoinError>,
    fatal: &mut Option<anyhow::Error>,
) {
    let error = match joined {
        Ok(Ok(())) => return,
        Ok(Err(error)) => {
            tracing::warn!(%error, "worker slot stopped during supervisor shutdown");
            error
        }
        Err(error) => {
            tracing::error!(%error, "worker slot panicked during supervisor shutdown");
            anyhow::anyhow!("supervisor slot panicked: {error}")
        }
    };
    if fatal.is_none() {
        *fatal = Some(error);
    }
}

fn request_drain(pid: u32) -> bool {
    signal_child(pid, libc::SIGUSR1)
}

fn signal_child(pid: u32, signal: i32) -> bool {
    let result = unsafe { libc::kill(pid as i32, signal) };
    if result != 0 {
        tracing::warn!(pid, signal, "failed to signal the worker process");
        false
    } else {
        true
    }
}

type LivePids = Arc<Mutex<HashMap<u32, u32>>>;

struct LiveGuard {
    live: LivePids,
    index: u32,
}

impl LiveGuard {
    fn insert(live: LivePids, index: u32, pid: u32) -> Self {
        live.lock().expect("live pids").insert(index, pid);
        Self { live, index }
    }
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.live.lock() {
            guard.remove(&self.index);
        }
    }
}

#[derive(Default)]
struct Pressure {
    drained: HashSet<u32>,
}

/// What the supervisor does when container memory crosses a threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAction {
    None,
    Drain,
    Kill,
}

/// `soft` and `hard` are fractions of `memory.max`, both below 1.
/// A soft ratio at or above the hard ratio is pulled down so drain still
/// happens before the kill.
pub fn memory_action(current: u64, limit: u64, soft: f64, hard: f64) -> MemoryAction {
    if limit == 0 || current == 0 {
        return MemoryAction::None;
    }
    if !(soft.is_finite() && hard.is_finite() && soft > 0.0 && hard > 0.0 && hard < 1.0) {
        return MemoryAction::None;
    }
    let soft = if soft < hard { soft } else { hard * 0.8 };
    let hard_bytes = threshold(limit, hard);
    let soft_bytes = threshold(limit, soft);
    if hard_bytes == 0 {
        return MemoryAction::None;
    }
    if current >= hard_bytes {
        MemoryAction::Kill
    } else if soft_bytes > 0 && current >= soft_bytes {
        MemoryAction::Drain
    } else {
        MemoryAction::None
    }
}

fn threshold(limit: u64, ratio: f64) -> u64 {
    let millis = (ratio * 1000.0).round() as u64;
    limit.saturating_mul(millis) / 1000
}

pub fn parse_memory_max(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "max" {
        None
    } else {
        raw.parse::<u64>().ok().filter(|value| *value > 0)
    }
}

pub fn cgroup_memory_dir(cgroup_text: &str) -> Option<std::path::PathBuf> {
    for line in cgroup_text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("0::") {
            let rest = rest.trim_start_matches('/');
            return Some(std::path::Path::new("/sys/fs/cgroup").join(rest));
        }
    }
    None
}

/// Kill the child that owns the memory. A small RSS means the usage is not
/// that process, so a kill would not stop the container OOM.
pub fn largest_child_to_kill(children: &[(u32, u64)], current: u64) -> Option<u32> {
    let (pid, rss) = children.iter().copied().max_by_key(|(_, rss)| *rss)?;
    if rss > 0 && rss.saturating_mul(2) >= current {
        Some(pid)
    } else {
        None
    }
}

/// RSS byte limits for one child. Either side may be omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildMemoryLimits {
    pub soft: Option<u64>,
    pub hard: Option<u64>,
}

/// A child-memory flag or environment value the supervisor will not run with.
#[derive(Debug)]
pub struct ChildMemoryConfigError {
    message: String,
}

impl ChildMemoryConfigError {
    fn invalid(raw: &str) -> Self {
        Self {
            message: format!(
                "child memory limit '{raw}' is invalid; expected a positive integer in bytes or with suffix K, M, or G"
            ),
        }
    }

    fn zero(raw: &str) -> Self {
        Self {
            message: format!(
                "child memory limit '{raw}' is invalid; the value must be greater than zero"
            ),
        }
    }

    fn soft_not_below(soft: &str, hard: &str) -> Self {
        Self {
            message: format!(
                "child memory limits are invalid; soft limit {soft} is not below hard limit {hard}"
            ),
        }
    }
}

impl std::fmt::Display for ChildMemoryConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ChildMemoryConfigError {}

const CHILD_MEMORY_PROM: &str = "simple_q_child_memory.prom";

/// The flag wins over the byte limits already stored on `Config`.
pub fn load_child_memory_limits(
    configured_soft: Option<u64>,
    configured_hard: Option<u64>,
    soft_flag: Option<&str>,
    hard_flag: Option<&str>,
) -> Result<ChildMemoryLimits, ChildMemoryConfigError> {
    let soft = match soft_flag {
        Some(raw) => Some(parse_byte_size(raw)?),
        None => configured_soft,
    };
    let hard = match hard_flag {
        Some(raw) => Some(parse_byte_size(raw)?),
        None => configured_hard,
    };
    if let (Some(left), Some(right)) = (soft, hard) {
        if left >= right {
            return Err(ChildMemoryConfigError::soft_not_below(
                &left.to_string(),
                &right.to_string(),
            ));
        }
    }
    Ok(ChildMemoryLimits { soft, hard })
}

/// `K`, `M`, and `G` are powers of 1024, with an optional `B`. A bare number
/// is bytes. Fractions, zero, and anything else are rejected.
pub fn parse_byte_size(raw: &str) -> Result<u64, ChildMemoryConfigError> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(ChildMemoryConfigError::invalid(raw));
    }
    let split_at = text
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    let (digits, suffix) = text.split_at(split_at);
    if digits.is_empty() {
        return Err(ChildMemoryConfigError::invalid(raw));
    }
    let suffix = suffix.trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "b" => 1_u64,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return Err(ChildMemoryConfigError::invalid(raw)),
    };
    let value: u64 = match digits.parse() {
        Ok(value) => value,
        Err(_) => return Err(ChildMemoryConfigError::invalid(raw)),
    };
    if value == 0 {
        return Err(ChildMemoryConfigError::zero(raw));
    }
    value
        .checked_mul(multiplier)
        .ok_or_else(|| ChildMemoryConfigError::invalid(raw))
}

/// What one child's RSS means for that pid alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteAction {
    None,
    Drain,
    Kill,
}

pub fn byte_action(rss: u64, soft: Option<u64>, hard: Option<u64>) -> ByteAction {
    if hard.is_some_and(|limit| rss >= limit) {
        ByteAction::Kill
    } else if soft.is_some_and(|limit| rss >= limit) {
        ByteAction::Drain
    } else {
        ByteAction::None
    }
}

/// Pids whose own RSS crossed a byte limit. A pid under both limits is left out.
pub fn byte_limit_targets(
    children: &[(u32, u64)],
    soft: Option<u64>,
    hard: Option<u64>,
) -> Vec<(u32, ByteAction)> {
    children
        .iter()
        .copied()
        .filter_map(|(pid, rss)| {
            let action = byte_action(rss, soft, hard);
            (action != ByteAction::None).then_some((pid, action))
        })
        .collect()
}

/// One child for the container soft ratio: the largest RSS not already drained.
/// The next call, after that pid is recorded, picks another child.
pub fn ratio_drain_target(children: &[(u32, u64)], already: &[u32]) -> Option<u32> {
    children
        .iter()
        .filter(|(pid, _)| !already.contains(pid))
        .max_by_key(|(_, rss)| *rss)
        .map(|(pid, _)| *pid)
}

/// One signal the supervisor will send on this memory tick.
///
/// A soft signal skips a pid whose delivery digit is not zero. A hard signal
/// does not read that digit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedSignal {
    Drain(u32, DrainCause),
    Kill(u32, KillCause),
}

/// Signals for one second.
///
/// Per-child limits, when set, compare each process with its own threshold.
/// Every child at the soft limit gets one `SIGUSR1`. Every child at the hard
/// limit gets `SIGKILL`. Several children can be named in the same plan.
///
/// The pod ratio adds at most one signal. At the soft ratio that is `SIGUSR1`
/// for the largest free RSS that has not already been asked to drain. A child
/// this plan is already draining counts as asked, so the next child waits for
/// the next call. A busy child is not asked and is not recorded as drained.
/// At the hard ratio the largest child is `SIGKILL`ed when its RSS is at
/// least half of `occupied`, whether or not its digit is set. If that child
/// is already in the plan for its own hard limit, no second child is killed.
/// When the largest is below half, nobody is killed for the ratio and the
/// largest free not-yet-asked child gets `SIGUSR1`.
pub fn plan_memory_signals(
    children: &[(u32, u64)],
    soft: Option<u64>,
    hard: Option<u64>,
    occupied: Option<u64>,
    limit: Option<u64>,
    soft_ratio: f64,
    hard_ratio: f64,
    already_drained: &[u32],
    busy: &[u32],
) -> Vec<PlannedSignal> {
    let mut planned = Vec::new();
    for &(pid, rss) in children {
        match byte_action(rss, soft, hard) {
            ByteAction::Kill => planned.push(PlannedSignal::Kill(pid, KillCause::MemoryHard)),
            ByteAction::Drain if !already_drained.contains(&pid) && !busy.contains(&pid) => {
                planned.push(PlannedSignal::Drain(pid, DrainCause::MemorySoft));
            }
            ByteAction::Drain | ByteAction::None => {}
        }
    }

    let (Some(occupied), Some(limit)) = (occupied, limit) else {
        return planned;
    };
    match memory_action(occupied, limit, soft_ratio, hard_ratio) {
        MemoryAction::None => {}
        MemoryAction::Kill => {
            if let Some(pid) = largest_child_to_kill(children, occupied) {
                if !planned.iter().any(
                    |signal| matches!(signal, PlannedSignal::Kill(current, _) if *current == pid),
                ) {
                    planned.retain(|signal| {
                        !matches!(signal, PlannedSignal::Drain(current, _) if *current == pid)
                    });
                    planned.push(PlannedSignal::Kill(pid, KillCause::MemoryRatio));
                }
            } else if let Some(pid) =
                ratio_drain_not_already_planned(children, already_drained, busy, &planned)
            {
                planned.push(PlannedSignal::Drain(pid, DrainCause::MemoryRatio));
            }
        }
        MemoryAction::Drain => {
            if let Some(pid) =
                ratio_drain_not_already_planned(children, already_drained, busy, &planned)
            {
                planned.push(PlannedSignal::Drain(pid, DrainCause::MemoryRatio));
            }
        }
    }
    planned
}

/// Largest free RSS that has not been asked to drain and is not already in `planned`.
/// A child this plan is killing is skipped. A busy child is skipped and is not
/// marked drained. A child this plan is already draining is the one for this
/// second, so the caller adds nobody else.
fn ratio_drain_not_already_planned(
    children: &[(u32, u64)],
    already_drained: &[u32],
    busy: &[u32],
    planned: &[PlannedSignal],
) -> Option<u32> {
    let killing = |pid: u32| {
        planned
            .iter()
            .any(|signal| matches!(signal, PlannedSignal::Kill(current, _) if *current == pid))
    };
    let draining = |pid: u32| {
        planned
            .iter()
            .any(|signal| matches!(signal, PlannedSignal::Drain(current, _) if *current == pid))
    };
    let (pid, _) = children
        .iter()
        .copied()
        .filter(|(pid, _)| !already_drained.contains(pid) && !busy.contains(pid) && !killing(*pid))
        .max_by_key(|(_, rss)| *rss)?;
    if draining(pid) { None } else { Some(pid) }
}

/// `true` when `processes * hard` is greater than a finite cgroup `memory.max`.
pub fn hard_budget_exceeds_cgroup(processes: u32, hard: u64, memory_max: u64) -> bool {
    (processes as u64).saturating_mul(hard) > memory_max
}

/// Bytes in `inactive_file` from a cgroup v2 `memory.stat`. Absent means the
/// caller keeps `memory.current` unchanged.
pub fn inactive_file_bytes(stat: &str) -> Option<u64> {
    for line in stat.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some("inactive_file") {
            return parts.next()?.parse().ok();
        }
    }
    None
}

/// Working set: `memory.current` minus `inactive_file`, the kubelet subtraction.
/// Without `inactive_file`, the usage is `memory.current`.
pub fn working_set(current: u64, inactive_file: Option<u64>) -> u64 {
    match inactive_file {
        Some(inactive) => current.saturating_sub(inactive),
        None => current,
    }
}

pub fn child_memory_prom_text(drains: u64, kills: u64) -> String {
    format!(
        "\
# HELP simple_q_child_memory_drains_total Children drained because a memory limit was crossed.
# TYPE simple_q_child_memory_drains_total counter
simple_q_child_memory_drains_total {drains}
# HELP simple_q_child_memory_kills_total Children killed because a memory limit was crossed.
# TYPE simple_q_child_memory_kills_total counter
simple_q_child_memory_kills_total {kills}
"
    )
}

fn cgroup_dir() -> Option<std::path::PathBuf> {
    let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_memory_dir(&text)
}

fn finite_cgroup_memory_max() -> Option<u64> {
    let dir = cgroup_dir()?;
    let raw = std::fs::read_to_string(dir.join("memory.max")).ok()?;
    parse_memory_max(&raw)
}

fn container_memory() -> Option<(u64, u64)> {
    let dir = cgroup_dir()?;
    let current = std::fs::read_to_string(dir.join("memory.current")).ok()?;
    let current = current.trim().parse::<u64>().ok()?;
    let limit = std::fs::read_to_string(dir.join("memory.max")).ok()?;
    let limit = parse_memory_max(&limit)?;
    let stat = std::fs::read_to_string(dir.join("memory.stat")).unwrap_or_default();
    Some((working_set(current, inactive_file_bytes(&stat)), limit))
}

fn resident_bytes(pid: u32) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        macos_resident_bytes(pid)
    }
    #[cfg(not(target_os = "macos"))]
    {
        proc_resident_bytes(pid)
    }
}

fn proc_resident_bytes(pid: u32) -> Option<u64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let pages = text.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = if page > 0 { page as u64 } else { 4096 };
    Some(pages.saturating_mul(page))
}

// `ri_resident_size` from libproc. `rusage_info_t` is a `void *` typedef, and
// the call writes the struct into the buffer behind that pointer.
#[cfg(target_os = "macos")]
fn macos_resident_bytes(pid: u32) -> Option<u64> {
    let mut info = unsafe { std::mem::zeroed::<libc::rusage_info_v0>() };
    let status = unsafe {
        libc::proc_pid_rusage(
            pid as libc::c_int,
            libc::RUSAGE_INFO_V0,
            &mut info as *mut libc::rusage_info_v0 as *mut libc::rusage_info_t,
        )
    };
    if status != 0 {
        return None;
    }
    Some(info.ri_resident_size)
}

fn log_child_memory_limits(limits: &ChildMemoryLimits) {
    tracing::info!(
        soft_bytes = %limit_label(limits.soft),
        hard_bytes = %limit_label(limits.hard),
        "effective child memory limits"
    );
}

fn limit_label(value: Option<u64>) -> String {
    match value {
        Some(bytes) => bytes.to_string(),
        None => "unset".to_string(),
    }
}

fn warn_if_hard_budget_exceeds_cgroup(processes: u32, hard: Option<u64>) {
    let Some(hard) = hard else {
        return;
    };
    let Some(memory_max) = finite_cgroup_memory_max() else {
        return;
    };
    if hard_budget_exceeds_cgroup(processes, hard, memory_max) {
        tracing::warn!(
            processes,
            hard_bytes = hard,
            memory_max,
            "processes times the hard child limit is greater than cgroup memory.max"
        );
    }
}

struct ChildMemoryMetrics {
    drains: AtomicU64,
    kills: AtomicU64,
    dir: std::path::PathBuf,
}

impl ChildMemoryMetrics {
    fn new(dir: std::path::PathBuf) -> Self {
        let metrics = Self {
            drains: AtomicU64::new(0),
            kills: AtomicU64::new(0),
            dir,
        };
        metrics.rewrite();
        metrics
    }

    fn record_drain(&self) {
        self.drains.fetch_add(1, Ordering::Relaxed);
        self.rewrite();
    }

    fn record_kill(&self) {
        self.kills.fetch_add(1, Ordering::Relaxed);
        self.rewrite();
    }

    fn rewrite(&self) {
        let drains = self.drains.load(Ordering::Relaxed);
        let kills = self.kills.load(Ordering::Relaxed);
        if let Err(error) = write_child_memory_prom(&self.dir, drains, kills) {
            tracing::warn!(%error, "failed to write simple_q_child_memory.prom");
        }
    }
}

fn write_child_memory_prom(dir: &std::path::Path, drains: u64, kills: u64) -> std::io::Result<()> {
    let path = dir.join(CHILD_MEMORY_PROM);
    let temporary = dir.join(format!("{CHILD_MEMORY_PROM}.tmp"));
    std::fs::write(&temporary, child_memory_prom_text(drains, kills))?;
    std::fs::rename(&temporary, &path)?;
    Ok(())
}

fn apply_memory_pressure(
    live: &LivePids,
    pressure: &mut Pressure,
    limits: ChildMemoryLimits,
    memory_soft_ratio: f64,
    memory_hard_ratio: f64,
    metrics: &ChildMemoryMetrics,
    roster: &Roster,
    digits: Option<&crate::slot_busy::SlotDigits>,
) {
    let slots: Vec<(u32, u32)> = live
        .lock()
        .expect("live pids")
        .iter()
        .map(|(slot, pid)| (*slot, *pid))
        .collect();
    let pids: Vec<u32> = slots.iter().map(|(_, pid)| *pid).collect();
    let busy: Vec<u32> = slots
        .iter()
        .filter(|(slot, _)| digits.is_some_and(|digits| digits.is_busy(*slot)))
        .map(|(_, pid)| *pid)
        .collect();
    pressure.drained.retain(|pid| pids.contains(pid));
    let children: Vec<(u32, u64)> = pids
        .iter()
        .copied()
        .filter_map(|pid| resident_bytes(pid).map(|rss| (pid, rss)))
        .collect();
    for (pid, rss) in &children {
        roster.set_rss(*pid, *rss);
    }

    let already: Vec<u32> = pressure.drained.iter().copied().collect();
    let (occupied, limit) = match container_memory() {
        Some((usage, limit)) => (Some(usage), Some(limit)),
        None => (None, None),
    };
    let planned = plan_memory_signals(
        &children,
        limits.soft,
        limits.hard,
        occupied,
        limit,
        memory_soft_ratio,
        memory_hard_ratio,
        &already,
        &busy,
    );
    for signal in planned {
        match signal {
            PlannedSignal::Kill(pid, cause) => {
                let rss = rss_of(&children, pid);
                match cause {
                    KillCause::MemoryHard => {
                        tracing::error!(reason = "memory hard", pid, rss, "killing child");
                    }
                    KillCause::MemoryRatio => {
                        tracing::error!(
                            pid,
                            rss,
                            usage = occupied.unwrap_or(0),
                            limit = limit.unwrap_or(0),
                            "container memory crossed the hard threshold; killing the largest child"
                        );
                    }
                }
                if signal_child(pid, libc::SIGKILL) {
                    metrics.record_kill();
                    roster.signal(pid, SentSignal::Kill(cause));
                }
            }
            PlannedSignal::Drain(pid, cause) => {
                if pressure.drained.contains(&pid) {
                    continue;
                }
                let rss = rss_of(&children, pid);
                match cause {
                    DrainCause::MemorySoft => {
                        tracing::warn!(reason = "memory soft", pid, rss, "draining child");
                    }
                    DrainCause::MemoryRatio => {
                        let usage = occupied.unwrap_or(0);
                        let cap = limit.unwrap_or(0);
                        if memory_action(usage, cap, memory_soft_ratio, memory_hard_ratio)
                            == MemoryAction::Kill
                        {
                            tracing::warn!(
                                pid,
                                rss,
                                usage,
                                limit = cap,
                                "container memory crossed the hard threshold but no child owns it; draining the largest child"
                            );
                        } else {
                            tracing::warn!(
                                pid,
                                rss,
                                usage,
                                limit = cap,
                                "container memory crossed the soft threshold; draining the largest child"
                            );
                        }
                    }
                    DrainCause::Shutdown => {}
                }
                if signal_child(pid, libc::SIGUSR1) {
                    pressure.drained.insert(pid);
                    metrics.record_drain();
                    roster.signal(pid, SentSignal::Usr1(cause));
                }
            }
        }
    }
}

fn exit_parts(status: &std::process::ExitStatus) -> (Option<i32>, Option<i32>) {
    use std::os::unix::process::ExitStatusExt;
    if let Some(signal) = status.signal() {
        (None, Some(signal))
    } else {
        (status.code(), None)
    }
}

#[derive(Clone, Default)]
struct Roster {
    inner: Arc<Mutex<RosterInner>>,
}

#[derive(Default)]
struct RosterInner {
    children: HashMap<u32, ChildTrack>,
    counters: Counters,
}

impl Roster {
    fn start(&self, slot: u32, pid: u32) {
        let mut inner = self.inner.lock().expect("roster");
        inner
            .children
            .entry(pid)
            .or_insert_with(|| ChildTrack::start(slot));
    }

    fn signal(&self, pid: u32, signal: SentSignal) {
        let mut inner = self.inner.lock().expect("roster");
        let Some(mut child) = inner.children.get(&pid).copied() else {
            return;
        };
        child.apply_signal(signal, &mut inner.counters);
        inner.children.insert(pid, child);
    }

    fn finish(&self, pid: u32, code: Option<i32>, term: Option<i32>) {
        let mut inner = self.inner.lock().expect("roster");
        let Some(mut child) = inner.children.get(&pid).copied() else {
            return;
        };
        // `ended_at` 0 means the exit is recorded and the next Redis `TIME`
        // supplies the timestamp. A local clock here can land before `started_at`
        // and make `prune_finished` drop the row on that same publish.
        child.apply_exit(code, term, 0, &mut inner.counters);
        inner.children.insert(pid, child);
    }

    fn set_rss(&self, pid: u32, rss: u64) {
        if let Some(child) = self.inner.lock().expect("roster").children.get_mut(&pid) {
            child.rss = Some(rss);
        }
    }

    fn snapshot(
        &self,
        now: u64,
        retain: u64,
    ) -> (Vec<simple_q_runtime::admin::ChildRecord>, Counters) {
        let mut inner = self.inner.lock().expect("roster");
        for child in inner.children.values_mut() {
            stamp_child_clocks(child, now);
        }
        let mut children: Vec<_> = inner
            .children
            .iter()
            .map(|(pid, track)| {
                let mut record = track.record();
                record.pid = *pid;
                record
            })
            .collect();
        prune_finished(&mut children, now, retain);
        let keep: HashSet<u32> = children.iter().map(|child| child.pid).collect();
        inner.children.retain(|pid, _| keep.contains(pid));
        (children, inner.counters)
    }
}

fn stamp_child_clocks(child: &mut ChildTrack, now: u64) {
    if child.started_at == 0 {
        child.started_at = now;
    }
    if child.ended_at == Some(0) {
        child.ended_at = Some(now);
    }
}

pub struct StopSignals {
    terminate: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
}

/// Install `SIGTERM` and `SIGINT` before any slow startup. The returned
/// listeners keep a signal that arrives while the caller is still importing
/// the application.
pub async fn arm_stop_signals() -> std::io::Result<StopSignals> {
    Ok(StopSignals {
        terminate: signal(SignalKind::terminate())?,
        interrupt: signal(SignalKind::interrupt())?,
    })
}

/// `true` when a stop signal is already recorded. Does not wait for a new one.
pub async fn stop_requested(signals: &mut StopSignals) -> bool {
    tokio::select! {
        biased;
        _ = signals.terminate.recv() => true,
        _ = signals.interrupt.recv() => true,
        _ = std::future::ready(()) => false,
    }
}

async fn retire_supervisor(
    stop: watch::Sender<bool>,
    heartbeat: tokio::task::JoinHandle<()>,
    redis_url: &str,
    host: &str,
    pid: u32,
) {
    let _ = stop.send(true);
    let _ = heartbeat.await;
    match redis_api::connect(redis_url).await {
        Ok(mut conn) => {
            if let Err(error) =
                simple_q_runtime::admin::forget_supervisor(&mut conn, host, pid).await
            {
                tracing::warn!(%error, "failed to remove the supervisor record");
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to remove the supervisor record");
        }
    }
}

fn spawn_supervisor_heartbeat(
    redis_url: String,
    settings: Config,
    host: String,
    pid: u32,
    mut stop: watch::Receiver<bool>,
    queues: Vec<String>,
    processes: u32,
    concurrency: u64,
    max_tasks: u64,
    limits: ChildMemoryLimits,
    roster: Roster,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let period = Duration::from_secs(settings.heartbeat_secs.max(1));
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut conn: Option<ConnectionManager> = None;
        let mut started_at = 0u64;
        let version = env!("CARGO_PKG_VERSION").to_owned();
        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => return,
                _ = tick.tick() => {}
            }
            if *stop.borrow() {
                return;
            }
            if conn.is_none() {
                match redis_api::connect(&redis_url).await {
                    Ok(connected) => conn = Some(connected),
                    Err(error) => {
                        tracing::warn!(%error, "supervisor redis connect failed");
                        continue;
                    }
                }
            }
            let Some(connection) = conn.as_mut() else {
                continue;
            };
            let now = match redis_seconds(connection).await {
                Ok(now) => now,
                Err(error) => {
                    tracing::warn!(%error, "supervisor heartbeat failed");
                    conn = None;
                    continue;
                }
            };
            if started_at == 0 {
                started_at = now;
            }
            let (cgroup_current, cgroup_max) = match container_memory() {
                Some((current, max)) => (Some(current), Some(max)),
                None => (None, None),
            };
            let (children, counters) = roster.snapshot(now, supervisor_ttl_secs(&settings));
            let record = SupervisorRecord {
                host: host.clone(),
                pid,
                started_at,
                version: version.clone(),
                queues: queues.clone(),
                processes,
                concurrency,
                max_tasks,
                child_soft: limits.soft,
                child_hard: limits.hard,
                cgroup_current,
                cgroup_max,
                recycled: counters.recycled,
                drains: counters.drains,
                kills: counters.kills,
                restarts: counters.restarts,
                children,
            };
            if let Err(error) = publish_supervisor(connection, &settings, &record).await {
                tracing::warn!(%error, "supervisor heartbeat failed");
                conn = None;
            }
        }
    })
}

async fn redis_seconds(conn: &mut ConnectionManager) -> Result<u64> {
    let (secs, _micros): (u64, u64) = redis::cmd("TIME")
        .query_async(conn)
        .await
        .context("failed to read Redis time")?;
    Ok(secs)
}

fn rss_of(children: &[(u32, u64)], pid: u32) -> u64 {
    children
        .iter()
        .find(|(id, _)| *id == pid)
        .map(|(_, rss)| *rss)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_sigterm_before_the_loop_is_still_waiting() {
        let mut signals = arm_stop_signals().await.expect("signal handler");
        unsafe { libc::raise(libc::SIGTERM) };
        let mut seen = false;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if stop_requested(&mut signals).await {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(seen, "SIGTERM was dropped before the supervisor loop");
    }

    #[test]
    fn an_exit_before_the_first_heartbeat_shares_one_redis_clock() {
        let mut child = ChildTrack::start(0);
        let mut counters = Counters::default();
        child.apply_exit(Some(0), None, 0, &mut counters);
        let redis_now = 1_700_000_000;
        stamp_child_clocks(&mut child, redis_now);
        assert_eq!(child.started_at, redis_now);
        assert_eq!(child.ended_at, Some(redis_now));
        assert!(!simple_q_runtime::admin::finished_row_expired(
            child.ended_at,
            redis_now,
            40
        ));
    }

    #[test]
    fn exit_code_3_is_fatal_and_other_codes_restart() {
        assert_eq!(classify_child_exit(true, Some(0)), ChildExit::Recycle);
        assert_eq!(classify_child_exit(false, Some(3)), ChildExit::Fatal);
        assert_eq!(classify_child_exit(false, Some(1)), ChildExit::Restart);
        assert_eq!(classify_child_exit(false, Some(2)), ChildExit::Restart);
        assert_eq!(classify_child_exit(false, None), ChildExit::Restart);
    }

    #[tokio::test]
    async fn a_panicked_slot_fails_supervisor_shutdown() {
        let joined = tokio::spawn(async {
            panic!("slot");
        })
        .await;
        let mut fatal = None;
        note_shutdown_join(joined, &mut fatal);
        let error = fatal.expect("a panicked slot was treated as success");
        assert!(error.to_string().contains("panicked"));
    }

    #[test]
    fn the_current_process_reports_a_resident_size() {
        let rss = resident_bytes(std::process::id()).expect("resident size");
        assert!(rss > 0);
    }

    #[test]
    fn memory_thresholds_drain_then_kill_and_ignore_an_unlimited_cgroup() {
        assert_eq!(memory_action(70, 100, 0.80, 0.95), MemoryAction::None);
        assert_eq!(memory_action(80, 100, 0.80, 0.95), MemoryAction::Drain);
        assert_eq!(memory_action(95, 100, 0.80, 0.95), MemoryAction::Kill);
        assert_eq!(memory_action(90, 100, 0.99, 0.90), MemoryAction::Kill);
        assert_eq!(memory_action(70, 100, 0.99, 0.90), MemoryAction::None);
        assert_eq!(memory_action(10, 0, 0.80, 0.95), MemoryAction::None);
        assert_eq!(memory_action(10, 100, 0.0, 0.95), MemoryAction::None);
        assert_eq!(parse_memory_max("max"), None);
        assert_eq!(parse_memory_max("  "), None);
        assert_eq!(parse_memory_max("1073741824"), Some(1_073_741_824));
        assert_eq!(
            cgroup_memory_dir("0::/kubepods/pod\n"),
            Some(std::path::PathBuf::from("/sys/fs/cgroup/kubepods/pod"))
        );
        assert_eq!(cgroup_memory_dir("1:name=systemd:/\n"), None);
    }

    #[test]
    fn only_a_child_that_owns_the_usage_is_killed() {
        let children = [(4, 10_u64), (9, 80)];
        assert_eq!(largest_child_to_kill(&children, 100), Some(9));
        assert_eq!(largest_child_to_kill(&[(4, 10)], 100), None);
        assert_eq!(largest_child_to_kill(&[], 100), None);
    }

    #[test]
    fn byte_sizes_accept_binary_units_and_reject_zero_junk_fractions_and_soft_above_hard() {
        assert_eq!(parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1500M").unwrap(), 1500 * 1024 * 1024);
        assert_eq!(parse_byte_size("512K").unwrap(), 512 * 1024);
        assert_eq!(parse_byte_size("4096").unwrap(), 4096);
        assert!(parse_byte_size("0").is_err());
        assert!(parse_byte_size("0M").is_err());
        assert!(parse_byte_size("abc").is_err());
        assert!(parse_byte_size("1.5G").is_err());
        assert!(parse_byte_size("1.5").is_err());
        assert!(load_child_memory_limits(None, None, Some("1G"), Some("1G")).is_err());
        assert!(load_child_memory_limits(None, None, Some("2G"), Some("1G")).is_err());
        assert!(load_child_memory_limits(None, None, Some("1500M"), Some("1G")).is_err());
        assert!(load_child_memory_limits(None, None, Some("1024K"), Some("1M")).is_err());
        let limits = load_child_memory_limits(None, None, Some("1G"), Some("1500M")).unwrap();
        assert!(limits.soft.unwrap() < limits.hard.unwrap());
        let soft_only = load_child_memory_limits(None, None, Some("512K"), None).unwrap();
        assert_eq!(soft_only.soft, Some(512 * 1024));
        assert_eq!(soft_only.hard, None);
        let hard_only = load_child_memory_limits(None, None, None, Some("1G")).unwrap();
        assert_eq!(hard_only.soft, None);
        assert_eq!(hard_only.hard, Some(1024 * 1024 * 1024));
        assert_eq!(
            load_child_memory_limits(None, None, None, None).unwrap(),
            ChildMemoryLimits {
                soft: None,
                hard: None,
            }
        );
        let zero = parse_byte_size("0").unwrap_err();
        assert!(zero.to_string().contains("greater than zero"));
        let junk = parse_byte_size("abc").unwrap_err();
        assert!(junk.to_string().contains("invalid"));
        let fraction = parse_byte_size("1.5G").unwrap_err();
        assert!(fraction.to_string().contains("invalid"));
        let order = load_child_memory_limits(None, None, Some("2G"), Some("1G")).unwrap_err();
        assert!(order.to_string().contains("not below"));
    }

    #[test]
    fn the_flag_wins_over_the_configured_limit() {
        let limits = load_child_memory_limits(Some(1), Some(2), Some("512K"), Some("1G")).unwrap();
        assert_eq!(limits.soft, Some(512 * 1024));
        assert_eq!(limits.hard, Some(1024 * 1024 * 1024));
        let configured =
            load_child_memory_limits(Some(512 * 1024), Some(1024 * 1024 * 1024), None, None)
                .unwrap();
        assert_eq!(configured.soft, Some(512 * 1024));
        assert_eq!(configured.hard, Some(1024 * 1024 * 1024));
        assert!(load_child_memory_limits(Some(2_000), Some(1_000), None, None).is_err());
    }

    #[test]
    fn byte_and_ratio_decisions_affect_one_pid_not_every_pid() {
        let children = [(11_u32, 100_u64), (22, 2_000), (33, 9_000)];
        let over_soft = byte_limit_targets(&children, Some(1_500), Some(20_000));
        assert_eq!(
            over_soft,
            vec![(22, ByteAction::Drain), (33, ByteAction::Drain)]
        );
        assert!(!over_soft.iter().any(|(pid, _)| *pid == 11));

        let mixed = byte_limit_targets(&children, Some(1_500), Some(8_000));
        assert_eq!(mixed, vec![(22, ByteAction::Drain), (33, ByteAction::Kill)]);
        assert_ne!(
            mixed.iter().map(|(pid, _)| *pid).collect::<Vec<_>>(),
            children.iter().map(|(pid, _)| *pid).collect::<Vec<_>>()
        );

        assert_eq!(
            byte_action(8_000, Some(1_500), Some(8_000)),
            ByteAction::Kill
        );
        assert_eq!(
            byte_action(1_500, Some(1_500), Some(8_000)),
            ByteAction::Drain
        );
        assert_eq!(
            byte_action(1_499, Some(1_500), Some(8_000)),
            ByteAction::None
        );
        assert_eq!(byte_action(9_000, None, None), ByteAction::None);
        assert_eq!(byte_action(9_000, Some(1_000), None), ByteAction::Drain);
        assert_eq!(byte_action(9_000, None, Some(8_000)), ByteAction::Kill);

        assert_eq!(ratio_drain_target(&children, &[]), Some(33));
        assert_eq!(ratio_drain_target(&children, &[33]), Some(22));
        assert_eq!(ratio_drain_target(&children, &[33, 22]), Some(11));
        assert_eq!(ratio_drain_target(&children, &[33, 22, 11]), None);
        let first = ratio_drain_target(&children, &[]).unwrap();
        assert_ne!(
            vec![first],
            children.iter().map(|(pid, _)| *pid).collect::<Vec<_>>()
        );
    }

    #[test]
    fn rss_selection_when_no_slot_is_busy() {
        let children = [(11_u32, 100_u64), (22, 2_000), (33, 9_000)];
        let soft = 0.80;
        let hard = 0.95;

        let over = plan_memory_signals(
            &children,
            Some(1_500),
            Some(8_000),
            None,
            None,
            soft,
            hard,
            &[],
            &[],
        );
        assert_eq!(
            over,
            vec![
                PlannedSignal::Drain(22, DrainCause::MemorySoft),
                PlannedSignal::Kill(33, KillCause::MemoryHard),
            ]
        );

        let first = plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[],
        );
        assert_eq!(
            first,
            vec![PlannedSignal::Drain(33, DrainCause::MemoryRatio)]
        );
        let second = plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[33],
            &[],
        );
        assert_eq!(
            second,
            vec![PlannedSignal::Drain(22, DrainCause::MemoryRatio)]
        );
        let third = plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[33, 22],
            &[],
        );
        assert_eq!(
            third,
            vec![PlannedSignal::Drain(11, DrainCause::MemoryRatio)]
        );
        assert!(
            plan_memory_signals(
                &children,
                None,
                None,
                Some(80),
                Some(100),
                soft,
                hard,
                &[33, 22, 11],
                &[],
            )
            .is_empty()
        );

        let owned = plan_memory_signals(
            &[(4, 10), (9, 80)],
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[],
        );
        assert_eq!(owned, vec![PlannedSignal::Kill(9, KillCause::MemoryRatio)]);
        let shared = plan_memory_signals(
            &[(4, 10), (9, 40)],
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[],
        );
        assert_eq!(
            shared,
            vec![PlannedSignal::Drain(9, DrainCause::MemoryRatio)]
        );
        assert_eq!(
            plan_memory_signals(
                &[(4, 10)],
                None,
                None,
                Some(100),
                Some(100),
                soft,
                hard,
                &[4],
                &[],
            ),
            Vec::<PlannedSignal>::new()
        );

        let both_large = [(1_u32, 60_u64), (2, 55)];
        assert_eq!(
            plan_memory_signals(
                &both_large,
                None,
                None,
                Some(100),
                Some(100),
                soft,
                hard,
                &[],
                &[],
            ),
            vec![PlannedSignal::Kill(1, KillCause::MemoryRatio)]
        );
        assert_eq!(
            plan_memory_signals(
                &[(1, 80), (2, 60)],
                None,
                Some(70),
                Some(100),
                Some(100),
                soft,
                hard,
                &[],
                &[],
            ),
            vec![PlannedSignal::Kill(1, KillCause::MemoryHard)]
        );

        let already_soft = plan_memory_signals(
            &children,
            Some(1_500),
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[],
        );
        assert_eq!(
            already_soft,
            vec![
                PlannedSignal::Drain(22, DrainCause::MemorySoft),
                PlannedSignal::Drain(33, DrainCause::MemorySoft),
            ]
        );
    }

    #[test]
    fn soft_signals_skip_a_busy_child() {
        let children = [(11_u32, 100_u64), (22, 2_000), (33, 9_000)];
        let soft = 0.80;
        let hard = 0.95;

        assert_eq!(
            plan_memory_signals(
                &children,
                None,
                None,
                Some(80),
                Some(100),
                soft,
                hard,
                &[],
                &[33],
            ),
            vec![PlannedSignal::Drain(22, DrainCause::MemoryRatio)]
        );
        assert_eq!(
            plan_memory_signals(
                &children,
                None,
                None,
                Some(80),
                Some(100),
                soft,
                hard,
                &[],
                &[33, 22],
            ),
            vec![PlannedSignal::Drain(11, DrainCause::MemoryRatio)]
        );
        assert!(
            plan_memory_signals(
                &children,
                None,
                None,
                Some(80),
                Some(100),
                soft,
                hard,
                &[],
                &[33, 22, 11],
            )
            .is_empty()
        );

        assert_eq!(
            plan_memory_signals(
                &children,
                Some(1_500),
                None,
                None,
                None,
                soft,
                hard,
                &[],
                &[33],
            ),
            vec![PlannedSignal::Drain(22, DrainCause::MemorySoft)]
        );
        assert_eq!(
            plan_memory_signals(
                &children,
                Some(1_500),
                None,
                Some(80),
                Some(100),
                soft,
                hard,
                &[],
                &[33],
            ),
            vec![PlannedSignal::Drain(22, DrainCause::MemorySoft)]
        );
        assert_eq!(
            plan_memory_signals(
                &children,
                Some(1_500),
                None,
                Some(80),
                Some(100),
                soft,
                hard,
                &[],
                &[33, 22],
            ),
            vec![PlannedSignal::Drain(11, DrainCause::MemoryRatio)]
        );

        assert_eq!(
            plan_memory_signals(
                &children,
                Some(1_500),
                Some(8_000),
                None,
                None,
                soft,
                hard,
                &[],
                &[33],
            ),
            vec![
                PlannedSignal::Drain(22, DrainCause::MemorySoft),
                PlannedSignal::Kill(33, KillCause::MemoryHard),
            ]
        );
        assert_eq!(
            plan_memory_signals(
                &[(9, 80)],
                None,
                None,
                Some(100),
                Some(100),
                soft,
                hard,
                &[],
                &[9],
            ),
            vec![PlannedSignal::Kill(9, KillCause::MemoryRatio)]
        );
        assert_eq!(
            plan_memory_signals(
                &[(9, 80), (4, 30)],
                None,
                None,
                Some(100),
                Some(100),
                soft,
                hard,
                &[],
                &[9],
            ),
            vec![PlannedSignal::Kill(9, KillCause::MemoryRatio)]
        );
        assert_eq!(
            plan_memory_signals(
                &[(9, 40), (4, 30)],
                None,
                None,
                Some(100),
                Some(100),
                soft,
                hard,
                &[],
                &[9],
            ),
            vec![PlannedSignal::Drain(4, DrainCause::MemoryRatio)]
        );
    }

    #[test]
    fn working_set_subtracts_inactive_file_before_the_ratio() {
        let stat = "anon 90\ninactive_file 20\nfile 40\n";
        assert_eq!(inactive_file_bytes(stat), Some(20));
        assert_eq!(inactive_file_bytes("anon 90\n"), None);
        assert_eq!(working_set(100, Some(20)), 80);
        assert_eq!(working_set(100, None), 100);
        assert_eq!(working_set(10, Some(40)), 0);
        let usage = working_set(100, inactive_file_bytes(stat));
        assert_eq!(memory_action(usage, 100, 0.80, 0.95), MemoryAction::Drain);
        assert_eq!(memory_action(100, 100, 0.80, 0.95), MemoryAction::Kill);
    }

    #[test]
    fn a_hard_budget_above_the_cgroup_is_only_a_warning_condition() {
        assert!(hard_budget_exceeds_cgroup(2, 100, 150));
        assert!(!hard_budget_exceeds_cgroup(2, 100, 200));
        assert!(hard_budget_exceeds_cgroup(2, 100, 199));
    }

    #[test]
    fn child_memory_prom_file_rewrites_both_counters() {
        let dir =
            std::env::temp_dir().join(format!("simple-q-child-memory-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        write_child_memory_prom(&dir, 3, 1).unwrap();
        let text = std::fs::read_to_string(dir.join("simple_q_child_memory.prom")).unwrap();
        assert!(text.contains("simple_q_child_memory_drains_total 3\n"));
        assert!(text.contains("simple_q_child_memory_kills_total 1\n"));
        write_child_memory_prom(&dir, 4, 2).unwrap();
        let text = std::fs::read_to_string(dir.join("simple_q_child_memory.prom")).unwrap();
        assert!(text.contains("simple_q_child_memory_drains_total 4\n"));
        assert!(text.contains("simple_q_child_memory_kills_total 2\n"));
        assert!(!text.contains("simple_q_child_memory_drains_total 3\n"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
