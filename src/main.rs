mod bridge;
mod child;
mod cli;
mod commands;
mod controller;
mod job;
mod lock;
mod names;
mod redis_api;
mod registry;
mod runtime;
mod supervisor;
mod worker;

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
                redis_api::enqueue(queue, task, payload).await
            })
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
