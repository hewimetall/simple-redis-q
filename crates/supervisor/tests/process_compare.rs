//! Process behavior of `tokio::process` and `watchexec-supervisor` 6.0.0.
//!
//! The child is Python. It writes READY, handles SIGUSR1, exits with a status,
//! and can fork a sleeper so a process-group kill can be observed.

use std::fs;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::process::Command as TokioCommand;
use watchexec_supervisor::ProcessEnd;
use watchexec_supervisor::Signal;
use watchexec_supervisor::command::{Command, Program, SpawnOptions};
use watchexec_supervisor::job::{CommandState, start_job};

const SCRIPT: &str = r#"
import os, signal, sys, time
mode = os.environ["SIMPLE_Q_FIXTURE"]
ready = os.environ.get("SIMPLE_Q_READY", "")
def mark(text):
    if ready:
        with open(ready, "w", encoding="utf-8") as handle:
            handle.write(text)
if mode == "exit":
    mark("READY")
    raise SystemExit(int(os.environ["SIMPLE_Q_CODE"]))
if mode == "usr1":
    def stop(*_args):
        raise SystemExit(0)
    signal.signal(signal.SIGUSR1, stop)
    mark("READY")
    while True:
        time.sleep(0.05)
if mode == "group":
    pid = os.fork()
    if pid == 0:
        os.execv("/bin/sleep", ["sleep", "30"])
    mark(f"READY {pid}")
    while True:
        time.sleep(0.05)
if mode == "fd":
    fd = int(os.environ["SIMPLE_Q_FD"])
    try:
        os.fstat(fd)
        mark("open")
    except OSError:
        mark("closed")
"#;

fn scratch(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "simple-q-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn python(mode: &str, ready: &std::path::Path) -> TokioCommand {
    let mut command = TokioCommand::new("python3");
    command
        .arg("-c")
        .arg(SCRIPT)
        .env("SIMPLE_Q_FIXTURE", mode)
        .env("SIMPLE_Q_READY", ready)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

async fn wait_file(path: &std::path::Path) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(text) = fs::read_to_string(path)
            && !text.is_empty()
        {
            return text;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("fixture did not write {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn proc_state(pid: i32) -> String {
    fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_else(|_| "absent".to_owned())
}

/// `true` when the pid is gone or a zombie. A sleeping sleeper is still running.
fn process_stopped(pid: i32) -> bool {
    let Ok(text) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return true;
    };
    let Some((_, rest)) = text.rsplit_once(')') else {
        return false;
    };
    matches!(rest.split_whitespace().next(), Some("Z") | None)
}

fn clear_cloexec(fd: i32) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let updated = flags & !libc::FD_CLOEXEC;
    if unsafe { libc::fcntl(fd, libc::F_SETFD, updated) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[tokio::test]
async fn given_tokio_child_when_sigusr1_then_it_exits_after_ready() {
    // Arrange
    let ready = scratch("usr1");
    let mut child = python("usr1", &ready).spawn().expect("spawn");

    // Act
    let line = wait_file(&ready).await;
    let pid = child.id().expect("pid");
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGUSR1) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("wait")
        .expect("status");

    // Assert
    assert_eq!(line, "READY");
    assert!(status.success(), "{status}");
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_tokio_child_when_it_exits_7_then_status_is_7() {
    // Arrange
    let ready = scratch("exit");
    let mut child = python("exit", &ready)
        .env("SIMPLE_Q_CODE", "7")
        .spawn()
        .expect("spawn");

    // Act
    let line = wait_file(&ready).await;
    let status = child.wait().await.expect("status");

    // Assert
    assert_eq!(line, "READY");
    assert_eq!(status.code(), Some(7));
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_tokio_child_when_sigkill_then_wait_reports_the_signal() {
    // Arrange
    let ready = scratch("kill");
    let mut child = python("usr1", &ready).spawn().expect("spawn");
    let _ = wait_file(&ready).await;
    let pid = child.id().expect("pid");

    // Act
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGKILL) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("wait")
        .expect("status");

    // Assert
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    assert!(status.code().is_none());
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_tokio_group_when_the_group_is_killed_then_grandchild_is_gone() {
    // Arrange
    let ready = scratch("group");
    let mut child = python("group", &ready)
        .process_group(0)
        .spawn()
        .expect("spawn");
    let line = wait_file(&ready).await;
    let grandchild: i32 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let pid = child.id().expect("pid") as i32;

    // Act
    assert_eq!(unsafe { libc::kill(-pid, libc::SIGKILL) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("wait")
        .expect("status");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Assert
    assert!(!status.success());
    assert!(
        process_stopped(grandchild),
        "grandchild still running: {}",
        proc_state(grandchild)
    );
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_tokio_cloexec_fd_when_pre_exec_clears_it_then_child_sees_the_fd() {
    // Arrange
    let ready = scratch("fd");
    let mut held = fs::File::create(scratch("fd-body")).expect("file");
    held.write_all(b"slot").unwrap();
    let fd = held.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    let mut command = python("fd", &ready);
    command.env("SIMPLE_Q_FD", fd.to_string());
    unsafe {
        command.pre_exec(move || clear_cloexec(fd));
    }
    let mut child = command.spawn().expect("spawn");

    // Act
    let line = wait_file(&ready).await;
    let _ = child.wait().await;

    // Assert
    assert_eq!(line, "open");
    let _ = fs::remove_file(&ready);
}

fn watchexec(grouped: bool) -> (watchexec_supervisor::job::Job, tokio::task::JoinHandle<()>) {
    start_job(Arc::new(Command {
        program: Program::Exec {
            prog: "python3".into(),
            args: vec!["-c".into(), SCRIPT.into()],
        },
        options: SpawnOptions {
            grouped,
            ..SpawnOptions::default()
        },
    }))
}

async fn arm_env(
    job: &watchexec_supervisor::job::Job,
    mode: String,
    ready: PathBuf,
    extra: Vec<(String, String)>,
    clear_fd: Option<i32>,
) {
    job.set_spawn_hook(move |wrap, _| {
        let command = wrap.command_mut();
        command.env("SIMPLE_Q_FIXTURE", &mode);
        command.env("SIMPLE_Q_READY", &ready);
        command.stdout(Stdio::null());
        command.stderr(Stdio::null());
        for (key, value) in &extra {
            command.env(key, value);
        }
        if let Some(fd) = clear_fd {
            unsafe {
                command.pre_exec(move || clear_cloexec(fd));
            }
        }
    })
    .await;
}

async fn running_pid(job: &watchexec_supervisor::job::Job) -> u32 {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    job.run(move |ctx| {
        if let CommandState::Running { child, .. } = ctx.current {
            let _ = sender.send(child.id());
        }
    })
    .await;
    receiver.await.expect("pid").expect("child id")
}

async fn finished(job: &watchexec_supervisor::job::Job) -> ProcessEnd {
    job.to_wait().await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    job.run(move |ctx| {
        if let CommandState::Finished { status, .. } = ctx.current {
            let _ = sender.send(*status);
        }
    })
    .await;
    receiver.await.expect("status")
}

#[tokio::test]
async fn given_watchexec_child_when_sigusr1_then_it_exits_after_ready() {
    // Arrange
    let ready = scratch("wx-usr1");
    let (job, task) = watchexec(false);
    arm_env(&job, "usr1".into(), ready.clone(), Vec::new(), None).await;
    job.start().await;
    let line = wait_file(&ready).await;

    // Act
    job.signal(Signal::User1).await;
    let status = tokio::time::timeout(Duration::from_secs(5), finished(&job))
        .await
        .expect("wait");
    job.delete_now().await;
    let _ = task.await;

    // Assert
    assert_eq!(line, "READY");
    assert_eq!(status, ProcessEnd::Success);
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_watchexec_child_when_it_exits_7_then_status_is_7() {
    // Arrange
    let ready = scratch("wx-exit");
    let (job, task) = watchexec(false);
    arm_env(
        &job,
        "exit".into(),
        ready.clone(),
        vec![("SIMPLE_Q_CODE".into(), "7".into())],
        None,
    )
    .await;
    job.start().await;

    // Act
    let line = wait_file(&ready).await;
    let status = finished(&job).await;
    job.delete_now().await;
    let _ = task.await;

    // Assert
    assert_eq!(line, "READY");
    assert_eq!(status, ProcessEnd::ExitError(7.try_into().unwrap()));
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_watchexec_group_when_the_group_is_killed_then_grandchild_is_gone() {
    // Arrange
    let ready = scratch("wx-group");
    let (job, task) = watchexec(true);
    arm_env(&job, "group".into(), ready.clone(), Vec::new(), None).await;
    job.start().await;
    let line = wait_file(&ready).await;
    let grandchild: i32 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let pid = running_pid(&job).await as i32;

    // Act
    assert_eq!(unsafe { libc::kill(-pid, libc::SIGKILL) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(5), finished(&job))
        .await
        .expect("wait");
    tokio::time::sleep(Duration::from_millis(50)).await;
    job.delete_now().await;
    let _ = task.await;

    // Assert
    assert!(matches!(status, ProcessEnd::ExitSignal(_)));
    assert!(
        process_stopped(grandchild),
        "grandchild still running: {}",
        proc_state(grandchild)
    );
    let _ = fs::remove_file(&ready);
}

#[tokio::test]
async fn given_watchexec_cloexec_fd_when_spawn_hook_clears_it_then_child_sees_the_fd() {
    // Arrange
    let ready = scratch("wx-fd");
    let mut held = fs::File::create(scratch("wx-fd-body")).expect("file");
    held.write_all(b"slot").unwrap();
    let fd = held.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    let (job, task) = watchexec(false);
    arm_env(
        &job,
        "fd".into(),
        ready.clone(),
        vec![("SIMPLE_Q_FD".into(), fd.to_string())],
        Some(fd),
    )
    .await;
    job.start().await;

    // Act
    let line = wait_file(&ready).await;
    let _ = finished(&job).await;
    job.delete_now().await;
    let _ = task.await;

    // Assert
    assert_eq!(line, "open");
    let _ = fs::remove_file(&ready);
}
