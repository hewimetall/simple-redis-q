use clap::{ArgAction, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "agent-worker",
    version,
    about = "Rust runtime for registered Python async tasks"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Execute one actor directly, without Redis.
    Execute {
        /// Python application in module:attribute format.
        #[arg(long)]
        app: String,

        /// Call the attribute. It must return an App.
        #[arg(long)]
        factory: bool,

        /// Actor name on the App.
        #[arg(long, default_value = "demo")]
        task: String,

        /// Task identifier placed in the payload.
        #[arg(long, default_value = "demo-1")]
        task_id: String,
    },

    /// Serve the Apalis board. BOARD_TOKEN is required unless BOARD_INSECURE=1 on loopback.
    Admin,

    /// Serve the multiprocess metrics scrape on dramatiq_prom_host:dramatiq_prom_port.
    Metrics,

    /// Read live supervisors and queue depths. This process does not register or heartbeat.
    Info {
        /// Python App in module:attribute format.
        #[arg(long, env = "SIMPLE_Q_APP")]
        app: String,

        /// Call the attribute. It must return an App.
        /// `SIMPLE_Q_APP_FACTORY`, when set to any value, enables this the same way `--factory` does.
        #[arg(long, action = ArgAction::SetTrue)]
        factory: bool,

        /// Include supervisors on every host, and print orphans.
        #[arg(long)]
        all: bool,

        /// Queue to include. Repeat the flag to name more than one.
        #[arg(long = "queue")]
        queue: Vec<String>,

        /// Print the snapshot as JSON.
        #[arg(long)]
        json: bool,
    },

    /// Enqueue one JSON payload. Fails immediately when Redis is unavailable.
    Enqueue {
        /// Queue name. A lowercase word such as `default`.
        #[arg(long)]
        queue: String,

        /// Actor name registered on the application.
        #[arg(long)]
        task: String,

        /// JSON object passed to the task.
        #[arg(long, default_value = "{}")]
        payload: String,

        /// JSON object of string headers stored on the message.
        #[arg(long, default_value = "{}")]
        headers: String,

        /// Extra attempts after the first failure. Stored on the message.
        #[arg(long, default_value_t = 0)]
        max_retries: u32,

        /// Minimum retry delay in milliseconds. Stored on the message.
        #[arg(long, default_value_t = simple_q_runtime::names::DEFAULT_MIN_BACKOFF_MS)]
        min_backoff_ms: u64,

        /// Maximum retry delay in milliseconds. Stored on the message.
        #[arg(long, default_value_t = simple_q_runtime::names::DEFAULT_MAX_BACKOFF_MS)]
        max_backoff_ms: u64,
    },

    /// Run queue workers. Without `--child` this process only supervises children.
    Worker {
        /// Python App in module:attribute format.
        #[arg(long, env = "SIMPLE_Q_APP")]
        app: String,

        /// Call the attribute. It must return an App.
        /// `SIMPLE_Q_APP_FACTORY`, when set to any value, enables this the same way `--factory` does.
        #[arg(long, action = ArgAction::SetTrue)]
        factory: bool,

        /// Queues assigned to this process, comma-separated.
        #[arg(long, value_delimiter = ',', default_value = "default")]
        queues: Vec<String>,

        /// Child processes. Ignored when `--child` is set.
        #[arg(long, default_value_t = 1)]
        processes: u32,

        /// In-flight tasks per child process.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,

        /// Completed tasks after which a child drains and is replaced.
        #[arg(long, default_value_t = 500)]
        max_tasks_per_process: u64,

        /// Soft RSS limit for one child, such as `512K`, `1500M`, or `1G`.
        /// `K`, `M`, and `G` are powers of 1024. A bare number is bytes.
        /// Overrides the soft byte limit on the App config.
        #[arg(long)]
        child_memory_soft: Option<String>,

        /// Hard RSS limit for one child, such as `512K`, `1500M`, or `1G`.
        /// `K`, `M`, and `G` are powers of 1024. A bare number is bytes.
        /// Overrides the hard byte limit on the App config.
        #[arg(long)]
        child_memory_hard: Option<String>,

        /// Internal. The supervisor starts the embedded interpreter only in this mode.
        #[arg(long, hide = true)]
        child: bool,
    },
}

/// `--factory`, or `SIMPLE_Q_APP_FACTORY` set to any value, including `false`.
///
/// The environment variable is not a clap `env` value. Clap would parse
/// `SetTrue`'s default `"false"` with that parser and turn the flag on when
/// the variable is unset.
pub fn factory_enabled(flag: bool) -> bool {
    flag || std::env::var_os("SIMPLE_Q_APP_FACTORY").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_stays_off_until_the_flag_or_any_env_value() {
        let absent = Cli::try_parse_from(["agent-worker", "worker", "--app", "app:create"])
            .expect("worker parses");
        match absent.command {
            Command::Worker { factory, .. } => assert!(!factory),
            _ => panic!("worker"),
        }

        let flagged =
            Cli::try_parse_from(["agent-worker", "info", "--app", "app:create", "--factory"])
                .expect("info parses");
        match flagged.command {
            Command::Info { factory, .. } => assert!(factory),
            _ => panic!("info"),
        }

        let env_set = std::env::var_os("SIMPLE_Q_APP_FACTORY").is_some();
        assert_eq!(factory_enabled(false), env_set);
        assert!(factory_enabled(true));
    }
}
