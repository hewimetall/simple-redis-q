//! The child learns that the supervisor process is gone by waiting on a pidfd.
//!
//! Linux only. The supervisor opens one pidfd for its own pid before it starts
//! workers, keeps that descriptor cloexec, and lets each child clear cloexec in
//! `pre_exec`. The child does not open a pidfd by the numeric parent pid, does
//! not read the descriptor, and does not call `waitid` on the supervisor.
//!
//! `poll(2)` on a pidfd, from `pidfd_open(2)`: `POLLIN` once the task is a
//! zombie, `POLLHUP` once that zombie has been reaped. `read` returns
//! `EINVAL`. Tokio's `AsyncFd` is edge-triggered and wakes spuriously, so a
//! wakeup is not an exit. The wait polls with timeout 0 and treats only
//! `POLLIN`, `POLLHUP`, or `POLLERR` as [`ParentExited`]. `WouldBlock` clears
//! the Tokio readiness so a live supervisor does not spin. The same poll runs
//! before the descriptor is registered and again after, so an exit that
//! happened before `ready().await` is not lost, including after the supervisor
//! has been reaped.
//!
//! A kernel where `pidfd_open` fails does not start workers. There is no
//! `/proc` or numeric-pid fallback. Other operating systems do not pass a
//! descriptor; a child started without `SIMPLE_Q_PARENT_PIDFD` runs as before.
//! A child that sees the variable but cannot watch it exits with an error.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Inherited by a worker. Absent when this operating system has no pidfd, or
/// when the binary was started by hand.
pub(crate) const ENV: &str = "SIMPLE_Q_PARENT_PIDFD";

/// One observation that the supervisor process has exited.
///
/// Set from the pidfd task. [`crate::child`] reads it and requests the same
/// drain as `SIGUSR1`. It does not cancel the worker monitor.
#[derive(Clone)]
pub(crate) struct Signal {
    inner: Arc<Inner>,
}

struct Inner {
    exited: AtomicBool,
    watching: AtomicBool,
    failed: Mutex<Option<String>>,
    notify: Notify,
}

impl Signal {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                exited: AtomicBool::new(false),
                watching: AtomicBool::new(false),
                failed: Mutex::new(None),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn exited(&self) -> bool {
        self.inner.exited.load(Ordering::SeqCst)
    }

    pub(crate) fn failure(&self) -> Option<String> {
        self.inner
            .failed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn mark_exited(&self) {
        self.inner.exited.store(true, Ordering::SeqCst);
        self.wake();
    }

    fn mark_watching(&self) {
        self.inner.watching.store(true, Ordering::SeqCst);
        self.wake();
    }

    fn mark_failed(&self, error: std::io::Error) {
        *self
            .inner
            .failed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.to_string());
        self.wake();
    }

    fn wake(&self) {
        self.inner.notify.notify_waiters();
        self.inner.notify.notify_one();
    }

    /// Wakes when the pidfd task has entered its wait, or has already failed.
    async fn until_watching(&self) {
        loop {
            if self.inner.watching.load(Ordering::SeqCst) || self.failure().is_some() {
                return;
            }
            self.inner.notify.notified().await;
        }
    }

    pub(crate) async fn notified(&self) {
        self.inner.notify.notified().await;
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::sync::{Mutex, OnceLock};

    use anyhow::{Context, Result};
    use tokio::io::Interest;
    use tokio::io::unix::AsyncFd;

    use super::{ENV, Signal};

    /// One supervisor-owned pidfd. Cloexec stays set in this process.
    pub(crate) struct SupervisorPidfd {
        fd: OwnedFd,
    }

    impl SupervisorPidfd {
        pub(crate) fn raw(&self) -> i32 {
            self.fd.as_raw_fd()
        }
    }

    pub(crate) fn open_supervisor() -> Result<Option<std::sync::Arc<SupervisorPidfd>>> {
        match open_self() {
            Ok(fd) => Ok(Some(std::sync::Arc::new(SupervisorPidfd { fd }))),
            Err(error) => Err(anyhow::anyhow!(
                "pidfd_open of this supervisor failed ({error}); this kernel cannot report supervisor exit, and there is no /proc or numeric-pid fallback"
            )),
        }
    }

    fn open_self() -> std::io::Result<OwnedFd> {
        let pid = rustix::process::getpid();
        match rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::NONBLOCK) {
            Ok(fd) => Ok(fd),
            Err(rustix::io::Errno::INVAL) => {
                let fd = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())?;
                set_nonblock(fd.as_raw_fd())?;
                Ok(fd)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn claim_lock() -> &'static Mutex<()> {
        static CLAIM: OnceLock<Mutex<()>> = OnceLock::new();
        CLAIM.get_or_init(|| Mutex::new(()))
    }

    /// Take the inherited supervisor pidfd, once.
    ///
    /// No variable: this child was not started by a pidfd supervisor.
    /// A variable that is not a pidfd: error, and the descriptor is left open
    /// because it may be stdin or some other unrelated fd. A second call sees
    /// no variable and returns `None` without closing anything.
    pub(crate) fn claim() -> Result<Option<OwnedFd>> {
        let _guard = claim_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        claim_locked()
    }

    fn claim_locked() -> Result<Option<OwnedFd>> {
        let Some(raw) = std::env::var(ENV).ok() else {
            return Ok(None);
        };
        unsafe { std::env::remove_var(ENV) };
        let fd: i32 = raw
            .parse()
            .with_context(|| format!("{ENV} is {raw:?}, not a file descriptor"))?;
        if let Err(error) = probe(fd) {
            anyhow::bail!("{ENV}={fd} is not a pidfd ({error})");
        }
        // SAFETY: probe accepted this number as a pidfd. The supervisor
        // transferred it. Nothing else in this process owns it.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        set_cloexec(owned.as_raw_fd())
            .context("failed to restore CLOEXEC on the supervisor pidfd")?;
        Ok(Some(owned))
    }

    /// `pidfd_send_signal` with signal 0 checks the descriptor and sends nothing.
    /// `ESRCH` still means it is a pidfd: the supervisor is already gone.
    fn probe(fd: i32) -> std::io::Result<()> {
        loop {
            let rc = unsafe { libc::syscall(libc::SYS_pidfd_send_signal, fd, 0, 0, 0) };
            if rc == 0 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return match error.raw_os_error() {
                Some(libc::ESRCH | libc::EPERM) => Ok(()),
                _ => Err(error),
            };
        }
    }

    /// Register the pidfd on the current runtime and spawn the wait.
    ///
    /// Returns after that task has been polled, or after an exit that was
    /// already visible. The caller keeps running; the task does not.
    pub(crate) async fn serve(fd: OwnedFd, signal: Signal) -> Result<()> {
        match arm(fd).context("failed to register the supervisor pidfd")? {
            Arm::Exited => signal.mark_exited(),
            Arm::Watching(watch) => {
                let task_signal = signal.clone();
                tokio::spawn(async move {
                    task_signal.mark_watching();
                    match watch.wait().await {
                        Ok(ParentExited) => task_signal.mark_exited(),
                        Err(error) => task_signal.mark_failed(error),
                    }
                });
                signal.until_watching().await;
                if let Some(error) = signal.failure() {
                    anyhow::bail!("supervisor pidfd watch failed: {error}");
                }
            }
        }
        Ok(())
    }

    enum Arm {
        Exited,
        Watching(Watch),
    }

    struct Watch {
        fd: AsyncFd<OwnedFd>,
    }

    /// `ParentExited` is the only success value. The caller does not reap.
    struct ParentExited;

    fn arm(fd: OwnedFd) -> std::io::Result<Arm> {
        set_nonblock(fd.as_raw_fd())?;
        if pidfd_terminated(fd.as_raw_fd())? {
            return Ok(Arm::Exited);
        }
        // SAFETY: `fd` owns this descriptor, stays open, and `as_raw_fd` does
        // not change for the life of the AsyncFd.
        let async_fd = unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) }
            .map_err(|error| error.into_parts().1)?;
        if pidfd_terminated(async_fd.as_raw_fd())? {
            return Ok(Arm::Exited);
        }
        Ok(Arm::Watching(Watch { fd: async_fd }))
    }

    impl Watch {
        async fn wait(self) -> std::io::Result<ParentExited> {
            let Watch { fd } = self;
            loop {
                let mut guard = fd.ready(Interest::READABLE).await?;
                match guard.try_io(|inner| match pidfd_terminated(inner.as_raw_fd())? {
                    true => Ok(ParentExited),
                    false => Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "supervisor pidfd is not terminated",
                    )),
                }) {
                    Ok(Ok(exited)) => return Ok(exited),
                    Ok(Err(error)) => return Err(error),
                    Err(_blocked) => continue,
                }
            }
        }
    }

    fn pidfd_terminated(fd: i32) -> std::io::Result<bool> {
        Ok(pidfd_readiness(fd)? != PidfdReadiness::Pending)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PidfdReadiness {
        Pending,
        /// `POLLIN` without `POLLHUP`: zombie, not yet reaped.
        Zombie,
        /// `POLLHUP`: the zombie was reaped. Still an exit. Do not wait for it.
        Reaped,
    }

    fn pidfd_readiness(fd: i32) -> std::io::Result<PidfdReadiness> {
        loop {
            let mut pollfd = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pollfd, 1, 0) };
            if rc < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if rc == 0 {
                return Ok(PidfdReadiness::Pending);
            }
            let revents = pollfd.revents;
            if revents & libc::POLLNVAL != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "supervisor pidfd is not pollable",
                ));
            }
            if revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
                return Ok(PidfdReadiness::Pending);
            }
            if revents & libc::POLLHUP != 0 {
                return Ok(PidfdReadiness::Reaped);
            }
            return Ok(PidfdReadiness::Zombie);
        }
    }

    fn set_nonblock(fd: i32) -> std::io::Result<()> {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn inherit(command: &mut tokio::process::Command, parent: &SupervisorPidfd) {
        let fd = parent.raw();
        command.env(ENV, fd.to_string());
        // SAFETY: runs in the forked child, before exec. `fcntl` of `F_GETFD`
        // and `F_SETFD` is async-signal-safe. The parent's descriptor stays cloexec.
        unsafe {
            command.pre_exec(move || crate::slot_busy::clear_cloexec(fd));
        }
    }

    fn set_cloexec(fd: i32) -> std::io::Result<()> {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn claim_for_test(value: Option<&str>) -> Result<Option<OwnedFd>> {
        let _guard = claim_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match value {
            Some(value) => unsafe { std::env::set_var(ENV, value) },
            None => unsafe { std::env::remove_var(ENV) },
        }
        let result = claim_locked();
        unsafe { std::env::remove_var(ENV) };
        result
    }

    #[cfg(test)]
    mod tests {
        use std::io::ErrorKind;
        use std::os::fd::AsRawFd;
        use std::os::unix::process::{CommandExt, ExitStatusExt};
        use std::process::{Child, Command, Stdio};
        use std::time::{Duration, Instant};

        use super::{PidfdReadiness, claim_for_test, open_self, pidfd_readiness};
        use crate::parent_pidfd::{ENV, Signal};
        use crate::slot_busy::clear_cloexec;

        const EVENT_WAIT: Duration = Duration::from_secs(10);
        const ALIVE_QUIET: Duration = Duration::from_millis(250);

        fn exclusive() -> std::sync::MutexGuard<'static, ()> {
            static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
            LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        #[test]
        fn a_bad_parent_pidfd_env_fails_and_leaves_the_fd_open() {
            let stdin_flags = unsafe { libc::fcntl(0, libc::F_GETFD) };
            assert!(stdin_flags >= 0, "stdin is open in the test process");

            let error = claim_for_test(Some("nope")).unwrap_err();
            assert!(
                error.to_string().contains("not a file descriptor"),
                "{error}"
            );

            let error = claim_for_test(Some("0")).unwrap_err();
            assert!(error.to_string().contains("not a pidfd"), "{error}");
            assert_eq!(unsafe { libc::fcntl(0, libc::F_GETFD) }, stdin_flags);

            let error = claim_for_test(Some("-1")).unwrap_err();
            assert!(error.to_string().contains("not a pidfd"), "{error}");
            assert!(claim_for_test(None).unwrap().is_none());
            assert_eq!(unsafe { libc::fcntl(0, libc::F_GETFD) }, stdin_flags);
        }

        #[test]
        fn repeated_spawns_do_not_grow_fds() {
            // Every pidfd in this process has the same /proc symlink text, so
            // the count is only meaningful while no other test holds one.
            let _exclusive = exclusive();
            let parent = open_self().unwrap();
            assert_eq!(pidfd_copies(parent.as_raw_fd()), 1);
            for _ in 0..20 {
                let mut command = Command::new("/usr/bin/true");
                pass_pidfd(&mut command, parent.as_raw_fd());
                let status = command.status().unwrap();
                assert!(status.success());
            }
            assert_eq!(pidfd_copies(parent.as_raw_fd()), 1);
            assert_cloexec(parent.as_raw_fd());

            let mut command = Command::new("/usr/bin/true");
            pass_pidfd(&mut command, parent.as_raw_fd());
            unsafe {
                command.pre_exec(|| Err(std::io::Error::other("forced pre_exec failure")));
            }
            let error = command.spawn().unwrap_err();
            assert!(
                error.to_string().contains("forced pre_exec failure")
                    || error.kind() != ErrorKind::NotFound,
                "{error}"
            );
            assert_eq!(pidfd_copies(parent.as_raw_fd()), 1);
            assert_cloexec(parent.as_raw_fd());
        }

        #[test]
        fn an_exec_from_the_child_does_not_inherit_the_pidfd() {
            let _exclusive = exclusive();
            let parent = open_self().unwrap();
            let (result_r, result_w) = pipe().unwrap();
            let mut command = harness();
            command
                .env("SIMPLE_Q_PIDFD_ROLE", "claim_exec")
                .env("SIMPLE_Q_PIDFD_RESULT_FD", result_w.to_string());
            pass_pidfd(&mut command, parent.as_raw_fd());
            unsafe {
                command.pre_exec(move || clear_cloexec(result_w));
            }
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            let mut child = command.spawn().unwrap();
            unsafe { libc::close(result_w) };
            let status = child.wait().unwrap();
            let mut buf = Vec::new();
            let line = read_line(result_r, &mut buf, EVENT_WAIT).unwrap();
            unsafe { libc::close(result_r) };
            assert_eq!(line.as_deref(), Some("absent"), "probe report {line:?}");
            assert!(status.success(), "{status}");
            assert_cloexec(parent.as_raw_fd());
        }

        #[test]
        fn a_living_parent_is_not_reported() {
            let mut session = start("hold").unwrap();
            session.expect("watching");
            let late = session.read_report(ALIVE_QUIET);
            assert!(late.is_none(), "living supervisor was reported: {late:?}");
        }

        #[test]
        fn a_normal_exit_is_reported() {
            let mut session = start("exit").unwrap();
            session.expect("watching");
            session.expect("exited");
            let status = session.group.child.wait().unwrap();
            assert!(status.success(), "{status}");
        }

        #[test]
        fn sigkill_of_the_parent_is_reported() {
            let mut session = start("hold").unwrap();
            session.expect("watching");
            let rc = unsafe { libc::kill(session.group.pid as i32, libc::SIGKILL) };
            assert_eq!(rc, 0);
            let status = session.group.child.wait().unwrap();
            assert_eq!(status.signal(), Some(libc::SIGKILL));
            session.expect("exited");
        }

        #[test]
        fn an_exit_before_the_wait_is_not_lost() {
            let _exclusive = exclusive();
            let mut session = start("release").unwrap();
            session.expect("spawned");
            wait_until_zombie(session.group.pid);
            let state = pidfd_state_of(session.group.pid);
            assert_eq!(state, PidfdReadiness::Zombie, "parent was reaped too early");
            session.write_go();
            session.expect("watching");
            session.expect("exited");
            let status = session.group.child.wait().unwrap();
            assert!(status.success(), "{status}");
        }

        #[test]
        fn a_reaped_parent_does_not_hang_the_wait() {
            let mut session = start("release").unwrap();
            session.expect("spawned");
            let status = session.group.child.wait().unwrap();
            assert!(status.success(), "{status}");
            session.write_go();
            session.expect("watching");
            session.expect("exited");
        }

        #[test]
        fn every_child_sees_the_same_supervisor_exit() {
            let mut session = start("two").unwrap();
            session.expect("watching");
            session.expect("watching");
            session.expect("exited");
            session.expect("exited");
            let status = session.group.child.wait().unwrap();
            assert!(status.success(), "{status}");
        }

        #[test]
        fn the_wait_resolves_when_the_parent_is_already_gone() {
            let _exclusive = exclusive();
            let mut child = Command::new("/usr/bin/sleep")
                .arg("30")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let pid = child.id();
            let owned = rustix::process::pidfd_open(
                rustix::process::Pid::from_raw(pid as i32).unwrap(),
                rustix::process::PidfdFlags::NONBLOCK,
            )
            .unwrap();
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
            let status = child.wait().unwrap();
            assert_eq!(status.signal(), Some(libc::SIGKILL));
            let signal = Signal::new();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime
                .block_on(super::serve(owned, signal.clone()))
                .unwrap();
            assert!(signal.exited());
        }

        #[test]
        #[ignore = "re-exec role for the pidfd process tests"]
        fn child_role() {
            let role = std::env::var("SIMPLE_Q_PIDFD_ROLE").unwrap_or_default();
            let result = match role.as_str() {
                "supervisor" => role_supervisor(),
                "watcher" => role_watcher(),
                "probe" => role_probe(),
                "claim_exec" => role_claim_exec(),
                "" => return,
                other => panic!("unknown pidfd role {other}"),
            };
            result.unwrap();
        }

        fn role_supervisor() -> std::io::Result<()> {
            if unsafe { libc::setpgid(0, 0) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let parent = open_self()?;
            let report: i32 = env_fd("SIMPLE_Q_PIDFD_REPORT_FD")?;
            let go: i32 = env_fd("SIMPLE_Q_PIDFD_GO_FD")?;
            let (status_r, status_w) = pipe()?;
            let scenario = std::env::var("SIMPLE_Q_PIDFD_SCENARIO").unwrap_or_default();
            let delayed = scenario == "release";
            let count = if scenario == "two" { 2 } else { 1 };
            for _ in 0..count {
                let child = spawn_watcher(parent.as_raw_fd(), report, status_w, go, delayed)?;
                // Dropping the handle does not kill the watcher. It has to
                // outlive this process so it can observe the pidfd.
                drop(child);
            }
            unsafe {
                libc::close(report);
                libc::close(go);
                libc::close(status_w);
            }
            let expected = if delayed { "spawned" } else { "watching" };
            let mut buf = Vec::new();
            for _ in 0..count {
                match read_line(status_r, &mut buf, EVENT_WAIT)? {
                    Some(line) if line == expected => {}
                    Some(line) => {
                        return Err(std::io::Error::other(format!(
                            "expected {expected}, got {line}"
                        )));
                    }
                    None => {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            format!("timed out waiting for {expected}"),
                        ));
                    }
                }
            }
            unsafe { libc::close(status_r) };
            if scenario == "hold" {
                loop {
                    std::thread::park();
                }
            }
            Ok(())
        }

        fn role_watcher() -> std::io::Result<()> {
            // The status pipe's reader goes away after the supervisor has seen
            // the ready line. A later write must return EPIPE, not die.
            unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
            let report = env_fd("SIMPLE_Q_PIDFD_REPORT_FD")?;
            let status = env_fd("SIMPLE_Q_PIDFD_STATUS_FD")?;
            let go = env_fd("SIMPLE_Q_PIDFD_GO_FD")?;
            let delayed = std::env::var("SIMPLE_Q_PIDFD_DELAY").is_ok();
            let owned = super::claim().map_err(|error| std::io::Error::other(error.to_string()))?;
            let owned = owned.ok_or_else(|| {
                std::io::Error::other("SIMPLE_Q_PARENT_PIDFD was not passed to the watcher")
            })?;
            if delayed {
                emit(report, status, "spawned");
                read_byte(go)?;
            }
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let signal = Signal::new();
            runtime
                .block_on(super::serve(owned, signal.clone()))
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            emit(report, status, "watching");
            runtime.block_on(async {
                loop {
                    if let Some(error) = signal.failure() {
                        return Err(std::io::Error::other(error));
                    }
                    if signal.exited() {
                        emit(report, status, "exited");
                        return Ok(());
                    }
                    signal.notified().await;
                }
            })
        }

        fn role_probe() -> std::io::Result<()> {
            let fd = env_fd("SIMPLE_Q_PIDFD_PROBE_FD")?;
            let result = env_fd("SIMPLE_Q_PIDFD_RESULT_FD")?;
            let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            let absent =
                rc < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EBADF);
            let word = if absent { "absent\n" } else { "present\n" };
            write_all(result, word.as_bytes())?;
            std::process::exit(if absent { 0 } else { 2 });
        }

        fn role_claim_exec() -> std::io::Result<()> {
            let owned = super::claim().map_err(|error| std::io::Error::other(error.to_string()))?;
            let owned = owned.ok_or_else(|| std::io::Error::other("missing supervisor pidfd"))?;
            let raw = owned.as_raw_fd();
            let mut command = harness();
            command
                .env("SIMPLE_Q_PIDFD_ROLE", "probe")
                .env("SIMPLE_Q_PIDFD_PROBE_FD", raw.to_string())
                .env_remove(ENV)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            let status = command.status()?;
            std::process::exit(status.code().unwrap_or(1));
        }

        fn spawn_watcher(
            pidfd: i32,
            report: i32,
            status: i32,
            go: i32,
            delayed: bool,
        ) -> std::io::Result<Child> {
            let mut command = harness();
            command
                .env("SIMPLE_Q_PIDFD_ROLE", "watcher")
                .env("SIMPLE_Q_PIDFD_REPORT_FD", report.to_string())
                .env("SIMPLE_Q_PIDFD_STATUS_FD", status.to_string())
                .env("SIMPLE_Q_PIDFD_GO_FD", go.to_string())
                .env(ENV, pidfd.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            if delayed {
                command.env("SIMPLE_Q_PIDFD_DELAY", "1");
            }
            unsafe {
                command.pre_exec(move || {
                    clear_cloexec(pidfd)?;
                    clear_cloexec(report)?;
                    clear_cloexec(status)?;
                    clear_cloexec(go)?;
                    Ok(())
                });
            }
            command.spawn()
        }

        struct Group {
            child: Child,
            pid: u32,
        }

        impl Drop for Group {
            fn drop(&mut self) {
                unsafe { libc::kill(-(self.pid as i32), libc::SIGKILL) };
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }

        struct Session {
            group: Group,
            report: i32,
            go: i32,
            buf: Vec<u8>,
        }

        impl Drop for Session {
            fn drop(&mut self) {
                unsafe {
                    libc::close(self.report);
                    libc::close(self.go);
                }
            }
        }

        impl Session {
            fn expect(&mut self, line: &str) {
                match self.read_report(EVENT_WAIT) {
                    Some(got) if got == line => {}
                    other => panic!("expected report {line:?}, got {other:?}"),
                }
            }

            fn read_report(&mut self, timeout: Duration) -> Option<String> {
                read_line(self.report, &mut self.buf, timeout).unwrap()
            }

            fn write_go(&self) {
                write_all(self.go, b"g").unwrap();
            }
        }

        fn start(scenario: &str) -> std::io::Result<Session> {
            let (report_r, report_w) = pipe()?;
            let (go_r, go_w) = pipe()?;
            let mut command = harness();
            command
                .env("SIMPLE_Q_PIDFD_ROLE", "supervisor")
                .env("SIMPLE_Q_PIDFD_SCENARIO", scenario)
                .env("SIMPLE_Q_PIDFD_REPORT_FD", report_w.to_string())
                .env("SIMPLE_Q_PIDFD_GO_FD", go_r.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            unsafe {
                command.pre_exec(move || {
                    clear_cloexec(report_w)?;
                    clear_cloexec(go_r)?;
                    Ok(())
                });
            }
            let child = command.spawn()?;
            let pid = child.id();
            unsafe {
                libc::close(report_w);
                libc::close(go_r);
            }
            Ok(Session {
                group: Group { child, pid },
                report: report_r,
                go: go_w,
                buf: Vec::new(),
            })
        }

        fn harness() -> Command {
            let mut command = Command::new(std::env::current_exe().expect("test executable"));
            command
                .arg("--exact")
                .arg("--ignored")
                .arg("--quiet")
                .arg("--color")
                .arg("never")
                .arg("parent_pidfd::linux::tests::child_role");
            command
        }

        fn pass_pidfd(command: &mut Command, fd: i32) {
            command.env(ENV, fd.to_string());
            unsafe {
                command.pre_exec(move || clear_cloexec(fd));
            }
        }

        fn pipe() -> std::io::Result<(i32, i32)> {
            let mut fds = [0; 2];
            if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // stdio setup dup2s onto 0, 1, and 2 after pre_exec. Keep the
            // inherited ends above those.
            Ok((move_above_stdio(fds[0])?, move_above_stdio(fds[1])?))
        }

        fn move_above_stdio(fd: i32) -> std::io::Result<i32> {
            if fd > 2 {
                return Ok(fd);
            }
            let duped = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
            if duped < 0 {
                return Err(std::io::Error::last_os_error());
            }
            unsafe { libc::close(fd) };
            Ok(duped)
        }

        fn env_fd(name: &str) -> std::io::Result<i32> {
            let raw = std::env::var(name)
                .map_err(|_| std::io::Error::other(format!("{name} is not set")))?;
            raw.parse()
                .map_err(|_| std::io::Error::other(format!("{name} is {raw:?}")))
        }

        fn emit(report: i32, status: i32, line: &str) {
            let mut bytes = Vec::from(line.as_bytes());
            bytes.push(b'\n');
            write_all(report, &bytes).expect("report");
            match write_all(status, &bytes) {
                Err(error) if error.kind() == ErrorKind::BrokenPipe => {}
                other => other.expect("status"),
            }
        }

        fn write_all(fd: i32, bytes: &[u8]) -> std::io::Result<()> {
            let mut rest = bytes;
            while !rest.is_empty() {
                let n = unsafe { libc::write(fd, rest.as_ptr().cast(), rest.len()) };
                if n < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                rest = &rest[n as usize..];
            }
            Ok(())
        }

        fn read_byte(fd: i32) -> std::io::Result<()> {
            let mut buf = [0u8; 1];
            loop {
                let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), 1) };
                if n > 0 {
                    return Ok(());
                }
                if n == 0 {
                    return Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "go pipe closed",
                    ));
                }
                let error = std::io::Error::last_os_error();
                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
        }

        fn read_line(
            fd: i32,
            buf: &mut Vec<u8>,
            timeout: Duration,
        ) -> std::io::Result<Option<String>> {
            let deadline = Instant::now() + timeout;
            loop {
                if let Some(index) = buf.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<u8> = buf.drain(..=index).collect();
                    let text = String::from_utf8_lossy(&line);
                    let text = text.trim_end_matches(['\r', '\n']);
                    if text.is_empty() {
                        continue;
                    }
                    return Ok(Some(text.to_string()));
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(None);
                }
                let mut pollfd = libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let millis = i32::try_from(remaining.as_millis()).unwrap_or(i32::MAX);
                let rc = unsafe { libc::poll(&mut pollfd, 1, millis) };
                if rc < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                if rc == 0 {
                    return Ok(None);
                }
                let mut tmp = [0u8; 256];
                let n = unsafe { libc::read(fd, tmp.as_mut_ptr().cast(), tmp.len()) };
                if n < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                if n == 0 {
                    return Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "report pipe closed",
                    ));
                }
                buf.extend_from_slice(&tmp[..n as usize]);
            }
        }

        fn wait_until_zombie(pid: u32) {
            let deadline = Instant::now() + EVENT_WAIT;
            loop {
                assert!(
                    Instant::now() < deadline,
                    "supervisor did not become a zombie"
                );
                if pidfd_state_of(pid) == PidfdReadiness::Zombie {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        fn pidfd_state_of(pid: u32) -> PidfdReadiness {
            let owned = rustix::process::pidfd_open(
                rustix::process::Pid::from_raw(pid as i32).unwrap(),
                rustix::process::PidfdFlags::NONBLOCK,
            )
            .unwrap();
            pidfd_readiness(owned.as_raw_fd()).unwrap()
        }

        fn pidfd_copies(fd: i32) -> usize {
            let target = std::fs::read_link(format!("/proc/self/fd/{fd}")).unwrap();
            std::fs::read_dir("/proc/self/fd")
                .unwrap()
                .filter(|entry| {
                    entry.as_ref().is_ok_and(|entry| {
                        std::fs::read_link(entry.path()).ok().as_ref() == Some(&target)
                    })
                })
                .count()
        }

        fn assert_cloexec(fd: i32) {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(flags >= 0, "{}", std::io::Error::last_os_error());
            assert!(flags & libc::FD_CLOEXEC != 0, "parent pidfd lost cloexec");
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{SupervisorPidfd, claim, inherit, open_supervisor, serve};

#[cfg(not(target_os = "linux"))]
mod other {
    use anyhow::Result;

    use super::ENV;

    pub(crate) struct SupervisorPidfd;

    pub(crate) fn open_supervisor() -> Result<Option<std::sync::Arc<SupervisorPidfd>>> {
        Ok(None)
    }

    pub(crate) fn claim() -> Result<Option<std::os::fd::OwnedFd>> {
        if std::env::var(ENV).is_ok() {
            unsafe { std::env::remove_var(ENV) };
            anyhow::bail!(
                "{ENV} is set, but this operating system cannot watch a supervisor pidfd"
            );
        }
        Ok(None)
    }

    pub(crate) async fn serve(_fd: std::os::fd::OwnedFd, _signal: super::Signal) -> Result<()> {
        anyhow::bail!("this operating system cannot watch a supervisor pidfd")
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) use other::{SupervisorPidfd, claim, open_supervisor, serve};
