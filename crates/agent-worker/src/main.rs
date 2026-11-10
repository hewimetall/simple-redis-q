mod bridge;
mod child;
mod cli;
mod commands;
mod runtime;
mod supervisor;
mod telemetry;
mod worker;

use std::collections::BTreeMap;

use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::names::{DEFAULT_TIME_LIMIT_MS, max_deliveries};
use simple_q_runtime::redis_api;

use anyhow::Context;
use clap::Parser;
use cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Stubs => {
            init_log();
            commands::stubs::run()
        }
        Command::Execute { app, task, task_id } => {
            init_log();
            runtime::initialize();
            commands::execute::run(&app, &task, &task_id)
        }
        Command::Admin => {
            init_log();
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(commands::admin::run())
        }
        Command::Enqueue {
            queue,
            task,
            payload,
            headers,
            max_retries,
            min_backoff_ms,
            max_backoff_ms,
        } => {
            init_log();
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(async {
                redis_api::install_from_env().await?;
                let kwargs = serde_json::from_str(&payload)?;
                let headers: BTreeMap<String, String> = serde_json::from_str(&headers)
                    .context("headers must be a JSON object of strings")?;
                let job = Job {
                    version: simple_q_runtime::MESSAGE_VERSION,
                    message_id: new_message_id(),
                    actor_name: task,
                    queue,
                    kwargs,
                    headers,
                    retries: 0,
                    max_retries,
                    time_limit_ms: DEFAULT_TIME_LIMIT_MS,
                    min_backoff_ms,
                    max_backoff_ms,
                    max_deliveries: max_deliveries(),
                    lock_key: None,
                    lock_ttl: 30,
                    skip_on_lock: false,
                };
                redis_api::enqueue_job(job, None).await.map(|_| ())
            })
        }
        Command::Metrics => {
            init_log();
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
            init_log();
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

fn init_log() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(crate::telemetry::fmt_env_filter())
        .try_init();
}
