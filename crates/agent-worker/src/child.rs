use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::runtime::Builder;
use tokio::signal::unix::{SignalKind, signal};

use simple_q_runtime::lock;
use simple_q_runtime::names::ping_budget;
use simple_q_runtime::ping::PingWatch;
use simple_q_runtime::redis_api;
use simple_q_runtime::registry;
use simple_q_runtime::python::adopt_runtime;

use crate::bridge;
use crate::runtime;
use crate::worker::{Shared, WorkerPool, run};

const PING_EVERY: Duration = Duration::from_secs(2);
const PING_TIMEOUT: Duration = Duration::from_secs(3);

pub fn run_child(
    app: String,
    queues: Vec<String>,
    concurrency: usize,
    max_tasks: u64,
) -> Result<()> {
    runtime::initialize();
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(runtime));
    pyo3_async_runtimes::tokio::init_with_runtime(runtime)
        .map_err(|_| anyhow::anyhow!("Tokio runtime was already installed"))?;

    adopt_runtime(runtime);
    runtime.block_on(async move {
        redis_api::install_from_env().await?;
        bridge::start(&app)?;
        bridge::call_hook("startup").await?;

        let shared = Shared {
            completed: Arc::new(AtomicU64::new(0)),
            draining: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            pool: WorkerPool::default(),
            max_tasks,
        };

        let terminate = signal(SignalKind::terminate())?;
        let user = signal(SignalKind::user_defined1())?;
        let lifecycle = lifecycle(shared.clone(), terminate, user);
        let workers = run(queues, concurrency, shared);

        tokio::pin!(lifecycle);
        tokio::select! {
            result = workers => result?,
            code = &mut lifecycle => {
                if code? == 2 {
                    tracing::error!(
                        "Python loop is unresponsive; leaving inflight messages for redelivery"
                    );
                    std::process::exit(2);
                }
            }
        }
        let _ = bridge::call_hook("shutdown").await;
        Ok(())
    })
}

async fn lifecycle(
    shared: Shared,
    mut terminate: tokio::signal::unix::Signal,
    mut user: tokio::signal::unix::Signal,
) -> Result<i32> {
    let mut watch = PingWatch::new(ping_budget());
    let mut next_ping = tokio::time::Instant::now();

    loop {
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
                tracing::info!("drain requested");
                shared.shutdown.store(true, Ordering::SeqCst);
                shared.pool.pause_all();
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if tokio::time::Instant::now() >= next_ping {
                    next_ping = tokio::time::Instant::now() + PING_EVERY;
                    let ok = bridge::ping(PING_TIMEOUT).await;
                    if watch.observe(ok, std::time::Instant::now()) {
                        lock::set_loop_healthy(false);
                        shared.pool.pause_all();
                        tracing::warn!(in_flight = registry::len(), "Python loop ping budget elapsed");
                        for execution in registry::snapshot() {
                            bridge::request_cancel(&execution.id);
                        }
                        return Ok(2);
                    }
                    lock::set_loop_healthy(ok);
                }
            }
        }
    }
}
