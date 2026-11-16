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
        include!("parent_pidfd_tests.rs");
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
