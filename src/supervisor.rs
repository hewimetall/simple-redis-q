use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

use crate::names::validate_queue;

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

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;

    let mut slots = Vec::with_capacity(processes as usize);
    for index in 0..processes {
        let app = app.clone();
        let queues = queues.clone();
        let shutdown = shutdown_rx.clone();
        slots.push(tokio::spawn(async move {
            supervise_slot(index, app, queues, concurrency, max_tasks, shutdown).await
        }));
    }

    tokio::select! {
        _ = terminate.recv() => {
            tracing::info!("SIGTERM received; draining child processes");
        }
        _ = interrupt.recv() => {
            tracing::info!("SIGINT received; draining child processes");
        }
        result = join_slots(&mut slots) => {
            result?;
            return Ok(());
        }
    }

    let _ = shutdown_tx.send(true);
    for slot in slots {
        slot.await.context("supervisor slot panicked")??;
    }
    Ok(())
}

async fn join_slots(slots: &mut Vec<tokio::task::JoinHandle<Result<()>>>) -> Result<()> {
    for slot in slots.drain(..) {
        slot.await.context("supervisor slot panicked")??;
    }
    Ok(())
}

async fn supervise_slot(
    index: u32,
    app: String,
    queues: Vec<String>,
    concurrency: usize,
    max_tasks: u64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        let mut child = spawn_child(&app, &queues, concurrency, max_tasks)?;
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
                if code.success() {
                    tracing::info!(index, pid, "worker process recycled");
                    backoff = Duration::from_secs(1);
                    continue;
                }
                tracing::warn!(index, pid, ?code, "worker process exited");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

fn spawn_child(app: &str, queues: &[String], concurrency: usize, max_tasks: u64) -> Result<Child> {
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
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn a worker process")
}

fn request_drain(pid: u32) {
    let result = unsafe { libc::kill(pid as i32, libc::SIGUSR1) };
    if result != 0 {
        tracing::warn!(pid, "failed to send SIGUSR1 to the worker process");
    }
}
