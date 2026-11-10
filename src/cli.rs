use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "agent-worker",
    version,
    about = "Rust runtime for Python task controllers"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Generate Python type stubs for the embedded Rust module.
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

    /// Serve the Apalis board. Loopback by default; other addresses require BOARD_TOKEN.
    Admin,

    /// Enqueue one JSON payload. Fails immediately when Redis is unavailable.
    Enqueue {
        /// Queue name. A lowercase word such as `control` or `fanout`.
        #[arg(long)]
        queue: String,

        /// Task name registered on the controller.
        #[arg(long)]
        task: String,

        /// JSON object passed to the task.
        #[arg(long, default_value = "{}")]
        payload: String,
    },

    /// Run queue workers. Without `--child` this process only supervises children.
    Worker {
        /// Python controller in module:attribute format.
        #[arg(long, default_value = "simple_q.app:app")]
        app: String,

        /// Queues assigned to this process, comma-separated.
        #[arg(long, value_delimiter = ',', default_value = "control")]
        queues: Vec<String>,

        /// Child processes. Ignored when `--child` is set.
        #[arg(long, default_value_t = 1)]
        processes: u32,

        /// In-flight tasks per child process.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,

        /// Completed tasks after which a child drains and is replaced.
        #[arg(long, default_value_t = 500)]
        max_tasks_per_process: u64,

        /// Internal. The supervisor starts the embedded interpreter only in this mode.
        #[arg(long, hide = true)]
        child: bool,
    },
}
