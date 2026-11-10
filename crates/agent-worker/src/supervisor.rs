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

    let mut slots = tokio::task::JoinSet::new();
    for index in 0..processes {
        let app = app.clone();
        let queues = queues.clone();
        let shutdown = shutdown_rx.clone();
        let shutdown_tx = shutdown_tx.clone();
        let prom_dir = prom_dir.clone();
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
            )
            .await
        });
    }

    let mut fatal = None;
    loop {
        tokio::select! {
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

        tokio::select! {
            changed = shutdown.changed() => {
                changed.context("shutdown watch closed")?;
                request_drain(pid);
                let _ = child.wait().await;
                return Ok(());
            }
            status = child.wait() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                let code = status.context("failed to wait for the worker process")?;
                match classify_child_exit(code.success(), code.code()) {
                    ChildExit::Recycle => {
                        tracing::info!(index, pid, "worker process recycled");
                        backoff = Duration::from_secs(1);
                        continue;
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
}
