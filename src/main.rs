mod cli;
mod commands;
mod controller;
mod runtime;

use clap::Parser;
use cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    runtime::initialize();

    match cli.command {
        Command::Stubs => commands::stubs::run(),
        Command::Execute { app, task, task_id } => commands::execute::run(&app, &task, &task_id),
        Command::Admin => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(commands::admin::run())
        }
    }
}
