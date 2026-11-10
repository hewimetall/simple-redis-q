//! User side that only forks and reaps children.
//!
//! It does not take messages. Exit code 3 is a bad tracing config and stops
//! the supervisor. Any other non-zero code waits and starts that slot again.
//! A panic while the slots are draining is a supervisor failure.
//!
//! When the container has a finite `memory.max`, a soft ratio drains children
//! and a hard ratio SIGKILLs the largest one. That happens before the kernel
//! OOM, which on cgroup v2 kills every process in the container.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

use simple_q_runtime::names::validate_queue;
use simple_q_runtime::slots::{reset_slot_gauges, wipe_db_files};

pub async fn run(
    app: String,
    queues: Vec<String>,
    processes: u32,
    concurrency: usize,
    max_tasks: u64,
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

    let prom_dir = prom_dir();
    std::fs::create_dir_all(&prom_dir).context("failed to create the metrics directory")?;
    wipe_db_files(&prom_dir).context("failed to clear the metrics directory")?;
    let metrics = spawn_metrics(&prom_dir)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let live = LivePids::default();
    let mut pressure = Pressure::default();

    let mut slots = tokio::task::JoinSet::new();
    for index in 0..processes {
        let app = app.clone();
        let queues = queues.clone();
        let shutdown = shutdown_rx.clone();
        let shutdown_tx = shutdown_tx.clone();
        let prom_dir = prom_dir.clone();
        let live = live.clone();
        slots.spawn(async move {
            supervise_slot(
                index,
                app,
                queues,
                concurrency,
                max_tasks,
                shutdown,
                shutdown_tx,
                prom_dir,
                live,
            )
            .await
        });
    }

    let mut fatal = None;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                apply_memory_pressure(&live, &mut pressure);
            }
            _ = terminate.recv() => {
                tracing::info!("SIGTERM received; draining child processes");
                let _ = shutdown_tx.send(true);
                break;
            }
            _ = interrupt.recv() => {
                tracing::info!("SIGINT received; draining child processes");
                let _ = shutdown_tx.send(true);
                break;
            }
            joined = slots.join_next() => {
                match joined {
                    None => {
                        drop(metrics);
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
    match fatal {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn supervise_slot(
    index: u32,
    app: String,
    queues: Vec<String>,
    concurrency: usize,
    max_tasks: u64,
    mut shutdown: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
    prom_dir: std::path::PathBuf,
    live: LivePids,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        reset_slot_gauges(&prom_dir, index).context("failed to reset slot gauges")?;
        let mut child = spawn_child(&app, &queues, concurrency, max_tasks, index, &prom_dir)?;
        let pid = child.id().context("child process has no pid")?;
        tracing::info!(index, pid, "worker process started");

        let status = {
            let _registered = LiveGuard::insert(live.clone(), index, pid);
            tokio::select! {
                changed = shutdown.changed() => {
                    changed.context("shutdown watch closed")?;
                    request_drain(pid);
                    let _ = child.wait().await;
                    return Ok(());
                }
                status = child.wait() => status,
            }
        };
        if *shutdown.borrow() {
            return Ok(());
        }
        let code = status.context("failed to wait for the worker process")?;
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
    queues: &[String],
    concurrency: usize,
    max_tasks: u64,
    slot: u32,
    prom_dir: &std::path::Path,
) -> Result<Child> {
    let executable = std::env::current_exe().context("failed to resolve the worker executable")?;
    Command::new(executable)
        .arg("worker")
        .arg("--child")
        .arg("--app")
        .arg(app)
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
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn a worker process")
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

fn request_drain(pid: u32) {
    let result = unsafe { libc::kill(pid as i32, libc::SIGUSR1) };
    if result != 0 {
        tracing::warn!(pid, "failed to send SIGUSR1 to the worker process");
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

fn memory_ratios() -> (f64, f64) {
    (
        ratio("SIMPLE_Q_MEMORY_SOFT_RATIO", 0.80),
        ratio("SIMPLE_Q_MEMORY_HARD_RATIO", 0.95),
    )
}

fn ratio(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0 && *value < 1.0)
        .unwrap_or(default)
}

fn container_memory() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let dir = cgroup_memory_dir(&text)?;
    let current = std::fs::read_to_string(dir.join("memory.current")).ok()?;
    let limit = std::fs::read_to_string(dir.join("memory.max")).ok()?;
    let current = current.trim().parse::<u64>().ok()?;
    let limit = parse_memory_max(&limit)?;
    Some((current, limit))
}

fn resident_bytes(pid: u32) -> Option<u64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let pages = text.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = if page > 0 { page as u64 } else { 4096 };
    Some(pages.saturating_mul(page))
}

fn apply_memory_pressure(live: &LivePids, pressure: &mut Pressure) {
    let Some((current, limit)) = container_memory() else {
        return;
    };
    let (soft, hard) = memory_ratios();
    let pids: Vec<u32> = live.lock().expect("live pids").values().copied().collect();
    pressure.drained.retain(|pid| pids.contains(pid));
    match memory_action(current, limit, soft, hard) {
        MemoryAction::None => {}
        MemoryAction::Drain => drain_children(&pids, pressure, current, limit),
        MemoryAction::Kill => {
            let children: Vec<(u32, u64)> = pids
                .iter()
                .copied()
                .filter_map(|pid| resident_bytes(pid).map(|rss| (pid, rss)))
                .collect();
            if let Some(pid) = largest_child_to_kill(&children, current) {
                tracing::error!(
                    pid,
                    current,
                    limit,
                    "container memory crossed the hard threshold; killing the largest child"
                );
                let result = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
                if result != 0 {
                    tracing::warn!(pid, "failed to kill the child over the memory threshold");
                }
            } else {
                drain_children(&pids, pressure, current, limit);
            }
        }
    }
}

fn drain_children(pids: &[u32], pressure: &mut Pressure, current: u64, limit: u64) {
    for pid in pids {
        if pressure.drained.insert(*pid) {
            tracing::warn!(
                pid,
                current,
                limit,
                "container memory crossed the soft threshold; draining"
            );
            request_drain(*pid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
