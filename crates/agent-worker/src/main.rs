mod bridge;
mod child;
mod cli;
mod commands;
mod runtime;
mod supervisor;
mod worker;

use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::names::{
    DEFAULT_MAX_BACKOFF_MS, DEFAULT_MIN_BACKOFF_MS, DEFAULT_TIME_LIMIT_MS, max_deliveries,
};
use simple_q_runtime::redis_api;

use clap::Parser;
use cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Stubs => commands::stubs::run(),
        Command::Execute { app, task, task_id } => {
            runtime::initialize();
            commands::execute::run(&app, &task, &task_id)
        }
        Command::Admin => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(commands::admin::run())
        }
        Command::Enqueue {
            queue,
            task,
            payload,
        } => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(async {
                redis_api::install_from_env().await?;
                let kwargs = serde_json::from_str(&payload)?;
                let job = Job {
                    version: simple_q_runtime::MESSAGE_VERSION,
                    message_id: new_message_id(),
                    actor_name: task,
                    queue,
                    kwargs,
                    headers: Default::default(),
                    retries: 0,
                    max_retries: 0,
                    time_limit_ms: DEFAULT_TIME_LIMIT_MS,
                    min_backoff_ms: DEFAULT_MIN_BACKOFF_MS,
                    max_backoff_ms: DEFAULT_MAX_BACKOFF_MS,
                    max_deliveries: max_deliveries(),
                    lock_key: None,
                    lock_ttl: 30,
                    skip_on_lock: false,
                };
                redis_api::enqueue_job(job, None).await.map(|_| ())
            })
        }
        Command::Metrics => {
            runtime::initialize();
            commands::metrics::run()
        }
        Command::Worker {
            child: false,
            app,
            queues,
            processes,
            concurrency,
            max_tasks_per_process,
        } => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(supervisor::run(
                app,
                queues,
                processes,
                concurrency,
                max_tasks_per_process,
            ))
        }
        Command::Worker {
            child: true,
            app,
            queues,
            concurrency,
            max_tasks_per_process,
            ..
        } => child::run_child(app, queues, concurrency, max_tasks_per_process),
    }
}
