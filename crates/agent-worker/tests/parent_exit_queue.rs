//! Supervisor `SIGKILL` drains the child through the existing ack path.
//!
//! Redis has to answer `PING`. A missing server is a failed test.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn killing_the_supervisor_drains_the_accepted_task() {
    let url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string());
    let pong = redis(&url, &["PING"]);
    assert_eq!(pong, "PONG", "Redis at {url} did not answer PING");

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut paths = vec![root.join("python"), root.join("python/tests")];
    if let Ok(existing) = std::env::var("PYTHONPATH") {
        paths.extend(std::env::split_paths(&existing).filter(|path| !path.as_os_str().is_empty()));
    }
    let pythonpath = std::env::join_paths(&paths)
        .unwrap_or_else(|error| panic!("PYTHONPATH cannot be joined from {paths:?}: {error}"));
    let import = Command::new("python3")
        .arg("-c")
        .arg("import simple_q, parent_exit_app")
        .env("PYTHONPATH", &pythonpath)
        .output()
        .expect("python3");
    assert!(
        import.status.success(),
        "simple_q is not importable:\n{}",
        String::from_utf8_lossy(&import.stderr)
    );

    let queue = format!("pexit{}", std::process::id());
    let dir = std::env::temp_dir().join(format!("parent-exit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let started = dir.join("started");
    let release = dir.join("release");
    let log_path = dir.join("worker.log");
    let log = std::fs::File::create(&log_path).unwrap();

    let bin = env!("CARGO_BIN_EXE_agent-worker");
    let mut command = Command::new(bin);
    command
        .arg("worker")
        .arg("--app")
        .arg("parent_exit_app:create_app")
        .arg("--factory")
        .arg("--queues")
        .arg(&queue)
        .arg("--processes")
        .arg("1")
        .arg("--concurrency")
        .arg("1")
        .arg("--max-tasks-per-process")
        .arg("100")
        .env("REDIS_URL", &url)
        .env("PYTHONPATH", &pythonpath)
        .env("PARENT_EXIT_QUEUE", &queue)
        .env("GATE_STARTED", &started)
        .env("GATE_RELEASE", &release)
        .env_remove("SIMPLE_Q_PARENT_PIDFD")
        .env_remove("AGENT_WORKER_CHILD")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    let child = command.spawn().expect("spawn supervisor");
    let supervisor_pid = child.id();
    let mut guard = Guard {
        child,
        supervisor_pid,
        worker: None,
        others: Vec::new(),
        log: log_path.clone(),
    };

    let enqueue = Command::new(bin)
        .arg("enqueue")
        .arg("--queue")
        .arg(&queue)
        .arg("--task")
        .arg("gate")
        .arg("--payload")
        .arg("{}")
        .env("REDIS_URL", &url)
        .output()
        .expect("enqueue");
    if !enqueue.status.success() {
        blow(
            &guard.log,
            &format!(
                "enqueue failed: {}\n{}",
                enqueue.status,
                String::from_utf8_lossy(&enqueue.stderr)
            ),
        );
    }

    let Some(worker) = wait_pid_file(&started, Duration::from_secs(60)) else {
        blow(&guard.log, "worker did not publish its pid");
    };
    // Record the pid before any failure so Drop can stop an orphan. The
    // kernel's /proc/pid/task/pid/children list is not a complete child list.
    guard.worker = Some(worker);
    let kids = children_of(supervisor_pid);
    guard.others = kids.iter().copied().filter(|pid| *pid != worker).collect();
    if !kids.contains(&worker) || !pid_alive(worker) {
        blow(
            &guard.log,
            &format!("worker {worker} is not a live child of {supervisor_pid}: {kids:?}"),
        );
    }

    let task_id = match wait_task_id(&url, &queue, Duration::from_secs(10)) {
        Some(id) => id,
        None => blow(&guard.log, "accepted task was not in an inflight set"),
    };

    let rc = unsafe { libc::kill(supervisor_pid as i32, libc::SIGKILL) };
    assert_eq!(
        rc,
        0,
        "kill supervisor: {}",
        std::io::Error::last_os_error()
    );
    let status = guard.child.wait().expect("wait supervisor");
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGKILL)
    );

    std::fs::File::create(&release)
        .unwrap()
        .write_all(b"go")
        .unwrap();
    if !wait_pid_exit(worker, Duration::from_secs(30)) {
        blow(
            &guard.log,
            "worker did not exit after the supervisor was killed",
        );
    }
    guard.worker = None;

    let score = redis(&url, &["ZSCORE", &format!("{queue}:done"), &task_id]);
    let meta = redis(
        &url,
        &["HGET", &format!("{queue}:meta:{task_id}"), "status"],
    );
    let inflight = inflight_members(&url, &queue);
    if score == "(nil)" || score.is_empty() || meta != "Done" || inflight.contains(&task_id) {
        blow(
            &guard.log,
            &format!(
                "task {task_id} was not acked: done score {score:?}, status {meta:?}, inflight {inflight:?}"
            ),
        );
    }
}

fn redis(url: &str, args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .arg("-u")
        .arg(url)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("redis-cli {} failed: {error}", args.join(" ")));
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "redis-cli {} failed: {stderr}{stdout}",
        args.join(" ")
    );
    stdout
}

fn inflight_members(url: &str, queue: &str) -> Vec<String> {
    let keys = redis(
        url,
        &["--scan", "--pattern", &format!("{queue}:inflight:*")],
    );
    let mut members = Vec::new();
    for key in keys.split_whitespace() {
        let listed = redis(url, &["SMEMBERS", key]);
        members.extend(listed.split_whitespace().map(str::to_string));
    }
    members
}

fn wait_task_id(url: &str, queue: &str, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let members = inflight_members(url, queue);
        if members.len() == 1 {
            return Some(members[0].clone());
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_pid_file(path: &Path, timeout: Duration) -> Option<u32> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(pid) = text.trim().parse::<u32>() {
                if pid > 0 {
                    return Some(pid);
                }
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_pid_exit(pid: u32, timeout: Duration) -> bool {
    let Some(raw) = rustix::process::Pid::from_raw(pid as i32) else {
        return false;
    };
    let owned = match rustix::process::pidfd_open(raw, rustix::process::PidfdFlags::NONBLOCK) {
        Ok(fd) => fd,
        Err(_) => return !pid_alive(pid),
    };
    let deadline = Instant::now() + timeout;
    loop {
        let mut pollfd = libc::pollfd {
            fd: std::os::fd::AsRawFd::as_raw_fd(&owned),
            events: libc::POLLIN,
            revents: 0,
        };
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        let millis = i32::try_from(left.as_millis().min(1_000)).unwrap_or(1_000);
        let rc = unsafe { libc::poll(&mut pollfd, 1, millis) };
        if rc > 0 && pollfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            return true;
        }
        if rc < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return !pid_alive(pid);
            }
        }
    }
}

fn pid_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn children_of(parent: u32) -> Vec<u32> {
    let mut kids = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return kids;
    };
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // `comm` is inside parentheses and may contain spaces. The ppid is
        // the field after the state, which follows the last `)`.
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let _state = fields.next();
        if fields.next().and_then(|text| text.parse().ok()) == Some(parent) {
            kids.push(pid);
        }
    }
    kids
}

struct Guard {
    child: Child,
    supervisor_pid: u32,
    worker: Option<u32>,
    others: Vec<u32>,
    log: PathBuf,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.supervisor_pid as i32, libc::SIGKILL) };
            let _ = self.child.wait();
        }
        if let Some(pid) = self.worker {
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        }
        for pid in &self.others {
            unsafe { libc::kill(*pid as i32, libc::SIGKILL) };
        }
    }
}

fn blow(log: &Path, message: &str) -> ! {
    let mut file = std::fs::File::open(log).ok();
    let mut text = String::new();
    if let Some(file) = file.as_mut() {
        let _ = file.read_to_string(&mut text);
    }
    let start = text.len().saturating_sub(4000);
    panic!("{message}\n--- worker log ---\n{}", &text[start..]);
}
