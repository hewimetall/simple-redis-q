use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "simple-q",
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

    /// Serve the Apalis board admin UI over the Redis backend.
    Admin,
}
