//! Spike: watchexec-supervisor 6.0.0 start → SIGUSR1 → wait → reap.
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
    job.signal(Signal::User1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    job.stop().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), job.to_wait()).await;
    job.delete_now().await;
    let _ = task.await;
    println!("spike-watchexec-job: ok watchexec-supervisor=6.0.0");
}
