//! One child process: interpreter, Redis workers, and the silence watch.
//!
//! Order is fixed. Claim the supervisor pidfd, import the application, and
//! read its tracing config before the subscriber exists, then start workers.
//! On a normal return, flush the exporter and only then call `shutdown` in
//! Python. Exit 2 does not flush.
//!
//! The pidfd wait is a task on this runtime, started before the application
//! import. Parent exit sets the same shutdown flag as `SIGUSR1`. It is not a
//! branch of the `select` that waits for workers: that branch would drop the
//! worker future and cancel accepted work.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::runtime::Builder;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

use simple_q_runtime::cleanup::sweep_queue;
use simple_q_runtime::lock;
use simple_q_runtime::ping::PingWatch;
use simple_q_runtime::python::adopt_runtime;
use simple_q_runtime::registry;

use crate::bridge;
use crate::runtime;
use crate::worker::{Shared, WorkerPool, run};

pub fn run_child(
    app: String,
    factory: bool,
    queues: Vec<String>,
    concurrency: usize,
    max_tasks: u64,
) -> Result<()> {
    // Take the descriptor before anything else can close or inherit it.
    // Restoring cloexec and dropping the env var happen inside claim.
    let parent_fd = crate::parent_pidfd::claim()?;
    crate::slot_busy::install()?;
    runtime::initialize();
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(runtime));
    pyo3_async_runtimes::tokio::init_with_runtime(runtime)
        .map_err(|_| anyhow::anyhow!("Tokio runtime was already installed"))?;

    adopt_runtime(runtime);
    let parent_exit = crate::parent_pidfd::Signal::new();
    if let Some(fd) = parent_fd {
        // The wait task is polled by the runtime workers from here on, including
        // while `bridge::start` imports the application on this thread.
        runtime
            .block_on(crate::parent_pidfd::serve(fd, parent_exit.clone()))
            .context("failed to watch the supervisor pidfd")?;
    }
    let tracing = match bridge::start(&app, factory) {
        Ok(tracing) => tracing,
        Err(error) => {
            if crate::telemetry::is_config_error(&error) {
                eprintln!("tracing configuration is invalid: {error:#}");
                std::process::exit(3);
            }
            return Err(error);
        }
    };
    if let Err(error) = crate::telemetry::install(tracing) {
        eprintln!("tracing configuration is invalid: {error:#}");
        std::process::exit(3);
    }
    runtime.block_on(async move {
        let (conn, config) = bridge::connect_client().await?;
        bridge::call_hook("startup").await?;

        let shared = Shared {
            completed: Arc::new(AtomicU64::new(0)),
            draining: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            pool: WorkerPool::default(),
            max_tasks,
            worker_names: Arc::new(Mutex::new(Vec::new())),
            redis: conn,
            config: config.clone(),
            stop_meta: Arc::new(watch::channel(false).0),
            announces: Arc::new(Mutex::new(Vec::new())),
            releases: Arc::new(Mutex::new(Vec::new())),
        };

        let terminate = signal(SignalKind::terminate())?;
        let user = signal(SignalKind::user_defined1())?;
        let lifecycle = lifecycle(
            shared.clone(),
            terminate,
            user,
            config.ping_budget(),
            config.ping_interval(),
            config.ping_timeout(),
            config.lifecycle_interval(),
            parent_exit,
        );
        let sweep_queues = queues.clone();
        let sweep_redis = shared.redis.clone();
        let sweep_config = config.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(sweep_config.sweep_interval());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_secs() as i64)
                    .unwrap_or(0);
                for queue in &sweep_queues {
                    if let Err(error) = sweep_queue(
                        &mut sweep_redis.clone(),
                        queue,
                        now,
                        sweep_config.done_ttl_secs,
                        sweep_config.dead_ttl_secs,
                        sweep_config.sweep_batch_size(),
                    )
                    .await
                    {
                        tracing::warn!(%error, %queue, "queue sweep failed");
                    }
                }
            }
        });
        let workers = run(queues, concurrency, shared.clone());

        tokio::pin!(lifecycle);
        // Parent exit must not be a branch here. It only sets `shutdown` inside
        // `lifecycle`. Dropping `workers` would cancel the monitor and the
        // accepted delivery.
        let worker_result = tokio::select! {
            result = workers => result,
            code = &mut lifecycle => {
                if code? == 2 {
                    tracing::error!(
                        "Python loop is unresponsive; leaving inflight messages for redelivery"
                    );
                    // Exit 2 does not flush. The interpreter may still hold the
                    // GIL, and pyo3 does not call Py_Finalize, so spans buffered
                    // for a stuck task are dropped.
                    std::process::exit(2);
                }
                Ok(())
            }
        };
        // A normal drain waits for idempotency deletes, then deletes the
        // metadata hash. A failed delete is logged and does not change the
        // exit code. SIGKILL never reaches this path.
        shared.delete_worker_metadata().await;
        // Push Rust spans into the Python exporter before on_stop. The
        // application's own force_flush stays in that hook.
        crate::telemetry::shutdown().await;
        let hook = bridge::call_hook("shutdown").await;
        worker_result?;
        hook?;
        Ok(())
    })
}

async fn lifecycle(
    shared: Shared,
    mut terminate: tokio::signal::unix::Signal,
    mut user: tokio::signal::unix::Signal,
    ping_budget: std::time::Duration,
    ping_interval: std::time::Duration,
    ping_timeout: std::time::Duration,
    lifecycle_interval: std::time::Duration,
    parent: crate::parent_pidfd::Signal,
) -> Result<i32> {
    let mut watch = PingWatch::new(ping_budget);
    let mut next_ping = tokio::time::Instant::now();

    loop {
        if let Some(error) = parent.failure() {
            anyhow::bail!("supervisor pidfd watch failed: {error}");
        }
        if parent.exited() && request_graceful_drain(&shared) {
            tracing::info!("supervisor exited; draining accepted work");
        }
        let stop_for_drain =
            shared.draining.load(Ordering::SeqCst) || shared.shutdown.load(Ordering::SeqCst);
        if stop_for_drain && !shared.pool.is_empty() && registry::len() == 0 {
            shared.pool.pause_all();
            shared.pool.stop_all();
        }

        tokio::select! {
            _ = terminate.recv() => {
                tracing::debug!("child ignored SIGTERM; the supervisor owns process recycling");
            }
            _ = user.recv() => {
                if request_graceful_drain(&shared) {
                    tracing::info!("drain requested");
                }
            }
            _ = parent.notified() => {
                // Exit and watch failure are handled at the top of the loop.
                // This arm does not finish `lifecycle`, so the caller keeps
                // waiting for accepted work.
            }
            _ = tokio::time::sleep(lifecycle_interval) => {
                if tokio::time::Instant::now() >= next_ping {
                    next_ping = tokio::time::Instant::now() + ping_interval;
                    let ok = bridge::ping(ping_timeout).await;
                    let budget_elapsed = watch.observe(ok, std::time::Instant::now());
                    // A missed ping leaves renewal running. The budget is what stops it.
                    lock::note_ping(ok, budget_elapsed);
                    if budget_elapsed {
                        shared.pool.pause_all();
                        tracing::warn!(in_flight = registry::len(), "Python loop ping budget elapsed");
                        for execution in registry::snapshot() {
                            bridge::request_cancel(&execution.id);
                        }
                        return Ok(2);
                    }
                }
            }
        }
    }
}

/// First transition into shutdown pauses workers. A second request, from
/// `SIGUSR1` and the pidfd together, does not pause again and does not run
/// cleanup twice. Cleanup stays after the worker monitor finishes.
fn request_graceful_drain(shared: &Shared) -> bool {
    request_drain_once(&shared.shutdown, || shared.pool.pause_all())
}

fn request_drain_once(shutdown: &AtomicBool, pause: impl FnOnce()) -> bool {
    if shutdown.swap(true, Ordering::SeqCst) {
        return false;
    }
    pause();
    true
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::request_drain_once;

    #[test]
    fn drain_is_requested_once() {
        let shutdown = AtomicBool::new(false);
        let pauses = AtomicUsize::new(0);
        let pause = || {
            pauses.fetch_add(1, Ordering::SeqCst);
        };
        assert!(request_drain_once(&shutdown, pause));
        assert!(!request_drain_once(&shutdown, pause));
        assert!(shutdown.load(Ordering::SeqCst));
        assert_eq!(pauses.load(Ordering::SeqCst), 1);
    }
}
