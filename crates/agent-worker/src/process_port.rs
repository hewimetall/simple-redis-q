//! Worker-slot spawn through `watchexec-supervisor` 6.0.0.
//!
//! Policy stays in `supervisor`. This module only starts one process, reports
//! its pid, and waits for the exit status. `KillOnDrop` is the crate default.
//! Drop before `wait` also sends `SIGKILL` so a cancelled slot does not leave
//! the child running until the job task is polled.

use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use watchexec_supervisor::ProcessEnd;
use watchexec_supervisor::command::{Command, Program, SpawnOptions};
use watchexec_supervisor::job::{CommandState, start_job};

use crate::parent_pidfd::SupervisorPidfd;

pub struct PortChild {
    job: watchexec_supervisor::job::Job,
    task: tokio::task::JoinHandle<()>,
    pid: u32,
    reaped: bool,
}

impl PortChild {
    pub fn id(&self) -> Option<u32> {
        Some(self.pid)
    }

    pub async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.job.to_wait().await;
        self.reaped = true;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.job
            .run(move |ctx| {
                if let CommandState::Finished { status, .. } = ctx.current {
                    let _ = sender.send(*status);
                }
            })
            .await;
        let status = receiver.await.map_err(std::io::Error::other)?;
        Ok(exit_status(status))
    }
}

impl Drop for PortChild {
    fn drop(&mut self) {
        if !self.reaped {
            unsafe {
                libc::kill(self.pid as i32, libc::SIGKILL);
            }
        }
        self.task.abort();
    }
}

pub struct WorkerSpawn {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub slot_fd: Option<i32>,
    pub parent_pidfd: Option<Arc<SupervisorPidfd>>,
}

pub async fn spawn_worker(spec: WorkerSpawn) -> Result<PortChild> {
    let (job, task) = start_job(Arc::new(Command {
        program: Program::Exec {
            prog: spec.program,
            args: spec.args,
        },
        options: SpawnOptions::default(),
    }));
    let slot_fd = spec.slot_fd;
    let parent = spec.parent_pidfd;
    let env = spec.env;
    job.set_spawn_hook(move |wrap, _| {
        let command = wrap.command_mut();
        for (key, value) in &env {
            command.env(key, value);
        }
        // The supervisor credential stays in the parent. A factory that needs
        // a URL reads SIMPLE_Q_APP_REDIS_URL, or builds the client in code.
        command.env_remove("REDIS_URL");
        if let Some(fd) = slot_fd {
            // SAFETY: the closure runs in the forked child, before exec. It
            // only calls fcntl. The parent descriptor stays cloexec.
            unsafe {
                command.pre_exec(move || crate::slot_busy::clear_cloexec(fd));
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(parent) = &parent {
            crate::parent_pidfd::inherit(command, parent);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = &parent;
    })
    .await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let sender = Arc::new(Mutex::new(Some(sender)));
    let hook_sender = Arc::clone(&sender);
    job.set_spawn_fn(move |command| {
        let child = command.spawn()?;
        if let Some(sender) = hook_sender.lock().expect("pid slot").take() {
            let _ = sender.send(child.id());
        }
        Ok(child)
    })
    .await;
    job.start().await;
    let pid = receiver
        .await
        .context("worker spawn did not report a pid")?
        .context("worker process has no pid")?;
    Ok(PortChild {
        job,
        task,
        pid,
        reaped: false,
    })
}

fn exit_status(end: ProcessEnd) -> ExitStatus {
    match end {
        ProcessEnd::Success => ExitStatus::from_raw(0),
        ProcessEnd::ExitError(code) => {
            let code = i32::try_from(code.get()).unwrap_or(1);
            ExitStatus::from_raw(code << 8)
        }
        ProcessEnd::ExitSignal(signal) => ExitStatus::from_raw(unix_signal(signal)),
        ProcessEnd::ExitStop(code) => ExitStatus::from_raw(code.get()),
        ProcessEnd::Exception(code) => ExitStatus::from_raw(code.get() << 8),
        ProcessEnd::Continued => ExitStatus::from_raw(0),
    }
}

fn unix_signal(signal: watchexec_supervisor::Signal) -> i32 {
    match signal {
        watchexec_supervisor::Signal::Hangup => libc::SIGHUP,
        watchexec_supervisor::Signal::ForceStop => libc::SIGKILL,
        watchexec_supervisor::Signal::Interrupt => libc::SIGINT,
        watchexec_supervisor::Signal::Quit => libc::SIGQUIT,
        watchexec_supervisor::Signal::Terminate => libc::SIGTERM,
        watchexec_supervisor::Signal::User1 => libc::SIGUSR1,
        watchexec_supervisor::Signal::User2 => libc::SIGUSR2,
        watchexec_supervisor::Signal::Suspend => libc::SIGSTOP,
        watchexec_supervisor::Signal::TerminalSuspend => libc::SIGTSTP,
        watchexec_supervisor::Signal::Continue => libc::SIGCONT,
        watchexec_supervisor::Signal::Custom(value) => value,
        _ => libc::SIGKILL,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_spawned_child_does_not_inherit_redis_url() {
        struct Restore(Option<String>);
        impl Drop for Restore {
            fn drop(&mut self) {
                unsafe {
                    match self.0.take() {
                        Some(value) => std::env::set_var("REDIS_URL", value),
                        None => std::env::remove_var("REDIS_URL"),
                    }
                }
            }
        }
        let restore = Restore(std::env::var("REDIS_URL").ok());
        unsafe {
            std::env::set_var("REDIS_URL", "redis://:supervisor-secret@127.0.0.1:6379/0");
        }
        let mut child = spawn_worker(WorkerSpawn {
            program: PathBuf::from("/usr/bin/python3"),
            args: vec![
                "-c".to_owned(),
                "import os,sys; sys.exit(0 if 'REDIS_URL' not in os.environ else 2)".to_owned(),
            ],
            env: Vec::new(),
            slot_fd: None,
            parent_pidfd: None,
        })
        .await
        .unwrap();
        let status = child.wait().await.unwrap();
        drop(restore);
        assert!(status.success(), "{status:?}");
    }
}
