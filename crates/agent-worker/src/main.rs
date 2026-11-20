//! User side of the process. Each command drives the library. The library
//! does not know which command started it.
//!
//! `worker` without `--child` only supervises. The child installs the
//! subscriber after the application import, so this file does not.

mod bridge;
mod child;
mod cli;
mod commands;
mod parent_pidfd;
mod process_port;
mod runtime;
mod slot_busy;
mod supervisor;
mod supervisor_telemetry;
mod telemetry;
mod worker;

use std::collections::BTreeMap;

use simple_q_runtime::config::Config;
use simple_q_runtime::job::{Job, new_message_id};
use simple_q_runtime::names::DEFAULT_TIME_LIMIT_MS;
use simple_q_runtime::redis_api;

use anyhow::Context;
use clap::Parser;
use cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Execute {
            app,
            factory,
            task,
            task_id,
        } => {
            init_log();
            runtime::initialize();
            commands::execute::run(&app, factory, &task, &task_id)
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
                let url = std::env::var("REDIS_URL").context("REDIS_URL is required")?;
                let settings = Config::from_env()?;
                let mut conn = redis_api::connect(&url).await?;
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
                    max_deliveries: settings.max_deliveries,
                    lock_key: None,
                    lock_ttl: 30,
                    skip_on_lock: false,
                    unique_key: None,
                };
                redis_api::enqueue_job(&mut conn, &settings, job, None)
                    .await
                    .map(|_| ())
            })
        }
        Command::Metrics => {
            init_log();
            runtime::initialize();
            commands::metrics::run()
        }
        Command::Info {
            app,
            factory,
            all,
            queue,
            json,
        } => {
            init_log();
            runtime::initialize();
            let factory = cli::factory_enabled(factory);
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(commands::info::run(app, factory, all, queue, json))
        }
        Command::Worker {
            child: false,
            app,
            factory,
            queues,
            processes,
            concurrency,
            max_tasks_per_process,
            child_memory_soft,
            child_memory_hard,
            otel_endpoint,
            otel_protocol,
            otel_service_name,
            otel_sampler,
            sentry_environment,
            sentry_release,
            sentry_sample_rate,
        } => {
            // The parent does not call runtime::initialize or bridge::read_client.
            // Telemetry is installed before the runtime so the header environment
            // can be restored before any other thread exists. Children inherit
            // that restored environment.
            let telemetry_config = match simple_q_parent::resolve_telemetry(
                &simple_q_parent::TelemetryFlags {
                    otel_endpoint,
                    otel_protocol,
                    otel_service_name,
                    otel_sampler,
                    sentry_environment,
                    sentry_release,
                    sentry_sample_rate,
                },
                |key| std::env::var(key).ok(),
                env!("CARGO_PKG_VERSION"),
            ) {
                Ok(config) => config,
                Err(error) => {
                    eprintln!("supervisor telemetry configuration is invalid: {error}");
                    std::process::exit(3);
                }
            };
            let url = match simple_q_parent::ParentBootstrap::from_env_and_cli(
                std::env::var("REDIS_URL").ok(),
            ) {
                Ok(boot) => boot.redis_url,
                Err(_) => {
                    eprintln!("REDIS_URL is required for the supervisor");
                    std::process::exit(3);
                }
            };
            let endpoint = match simple_q_parent::parse_redis_endpoint(&url) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    eprintln!("REDIS_URL is invalid: {error}");
                    std::process::exit(3);
                }
            };
            let settings = match Config::from_env() {
                Ok(settings) => settings,
                Err(error) => {
                    eprintln!("{error}");
                    std::process::exit(3);
                }
            };
            if let Err(error) = supervisor_telemetry::install(&telemetry_config) {
                eprintln!("supervisor telemetry configuration is invalid: {error}");
                std::process::exit(3);
            }
            let runtime = tokio::runtime::Runtime::new()?;
            let mut signals = runtime.block_on(supervisor::arm_stop_signals())?;
            let factory = cli::factory_enabled(factory);
            let limits = match supervisor::load_child_memory_limits(
                settings.child_memory_soft,
                settings.child_memory_hard,
                child_memory_soft.as_deref(),
                child_memory_hard.as_deref(),
            ) {
                Ok(limits) => limits,
                Err(error) => {
                    tracing::error!("{error}");
                    std::process::exit(3);
                }
            };
            runtime.block_on(supervisor::run(
                app,
                factory,
                queues,
                processes,
                concurrency,
                max_tasks_per_process,
                limits,
                settings.memory_soft_ratio,
                settings.memory_hard_ratio,
                url,
                endpoint.host,
                endpoint.db,
                settings,
                &mut signals,
            ))
        }
        Command::Worker {
            child: true,
            app,
            factory,
            queues,
            concurrency,
            max_tasks_per_process,
            ..
        } => child::run_child(
            app,
            cli::factory_enabled(factory),
            queues,
            concurrency,
            max_tasks_per_process,
        ),
    }
}

fn init_log() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(crate::telemetry::fmt_env_filter())
        .try_init();
}
