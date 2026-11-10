use clap::{Parser, Subcommand};

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
    /// Print where the checked-in Python stubs live. This does not generate them.
    Stubs,

    /// Execute one controller task directly, without Redis.
    Execute {
        /// Python application in module:attribute format.
        #[arg(long, default_value = "simple_q.app:app")]
        app: String,

        /// Task name passed to the controller.
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

    /// Enqueue one JSON payload. Fails immediately when Redis is unavailable.
    Enqueue {
        /// Queue name. A lowercase word such as `default` or `quota`.
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
        /// Python registry in module:attribute format.
        #[arg(long, default_value = "simple_q.app:app")]
        app: String,

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

        /// Internal. The supervisor starts the embedded interpreter only in this mode.
        #[arg(long, hide = true)]
        child: bool,
    },
}
