//! Spike: API smoke for watchexec-supervisor 6.0.0.
//!
//! This is **not** a graceful-drain proof for agent-worker.
//! It only checks that start / signal / stop / wait / delete compile and run
//! against `/bin/sleep`. A later process test must use a child that:
//! READY → SIGUSR1 → confirm drain → exit, then assert wait + exit status.
use std::sync::Arc;
use std::time::Duration;
use watchexec_supervisor::Signal;
use watchexec_supervisor::command::{Command, Program};
use watchexec_supervisor::job::start_job;

#[tokio::main]
async fn main() {
    let (job, task) = start_job(Arc::new(Command {
        program: Program::Exec {
            prog: "/bin/sleep".into(),
            args: vec!["2".into()],
        }
        .into(),
        options: Default::default(),
    }));

    job.start().await;
    // API availability only — sleep ignores SIGUSR1.
    job.signal(Signal::User1).await;
    job.stop().await;
    match tokio::time::timeout(Duration::from_secs(5), job.to_wait()).await {
        Ok(_) => {}
        Err(_) => {
            eprintln!("spike-watchexec-job: FAIL wait timed out (API smoke)");
            std::process::exit(1);
        }
    }
    job.delete_now().await;
    let _ = task.await;
    println!(
        "spike-watchexec-job: API smoke ok watchexec-supervisor=6.0.0 (not graceful-drain proof)"
    );
}
