//! One digit per worker slot, on a single anonymous page.
//!
//! The supervisor creates the memfd once and keeps it until it exits. A new
//! memfd per child would leak descriptors. The page has no name in `/dev/shm`.
//! Seals stop a child from shrinking it, so a mapping cannot `SIGBUS` on a
//! truncated file. Write is not sealed: both sides store the digit.
//!
//! The mapping does not survive `exec`. The descriptor does. `CLOEXEC` stays
//! set in the parent, so another thread's spawn does not inherit it. The
//! forked child clears `CLOEXEC` and the child, after `exec`, maps the page
//! and closes its descriptor. A process Python starts later must not be able
//! to write the digit.
//!
//! The cell is an `AtomicU32` count of deliveries in progress. `Relaxed` is
//! enough. `get_mut` and a plain store into the bytes are not used. Any
//! non-zero value means the slot is busy. The parent stores 0 before it
//! starts a child in that slot, so a 1 left by `SIGKILL` does not mark the
//! next child busy.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Context, Result};

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// Kernel ABI. libc 0.2.191 does not export these for linux-gnu.
#[cfg(target_os = "linux")]
const MFD_CLOEXEC: libc::c_uint = 0x0001;
#[cfg(target_os = "linux")]
const MFD_ALLOW_SEALING: libc::c_uint = 0x0002;

static DIGIT: OnceLock<&'static AtomicU32> = OnceLock::new();

/// Map the inherited descriptor when the supervisor passed one.
///
/// Absent means this process was not started as a supervised slot. A present
/// but unusable descriptor is an error: running without the digit would let a
/// soft signal drain a child the parent believes it can see.
pub fn install() -> Result<()> {
    if std::env::var_os("SIMPLE_Q_SLOT_FD").is_none() {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        install_linux()
    }
    #[cfg(not(target_os = "linux"))]
    {
        anyhow::bail!("SIMPLE_Q_SLOT_FD is set, but this system has no memfd slot digit")
    }
}

/// Add one delivery. Dropped when that delivery returns, including by panic.
/// `process::exit` skips the drop; the parent stores 0 before the next spawn.
pub fn enter() -> Option<BusyGuard> {
    let cell = *DIGIT.get()?;
    cell.fetch_add(1, Ordering::Relaxed);
    Some(BusyGuard { cell })
}

pub struct BusyGuard {
    cell: &'static AtomicU32,
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.cell.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Clear `FD_CLOEXEC` after fork and before exec. `fcntl` of these commands
/// is async-signal-safe. The parent descriptor stays cloexec.
pub fn clear_cloexec(fd: i32) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_linux() -> Result<()> {
    let raw = std::env::var("SIMPLE_Q_SLOT_FD").context("SIMPLE_Q_SLOT_FD")?;
    let fd: i32 = raw
        .parse()
        .context("SIMPLE_Q_SLOT_FD is not a descriptor")?;
    let held = CloseOnDrop(fd);
    let slot: u32 = std::env::var("SIMPLE_Q_SLOT")
        .context("SIMPLE_Q_SLOT")?
        .parse()
        .context("SIMPLE_Q_SLOT is not a slot index")?;
    let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
    if unsafe { libc::fstat(held.0, &mut stat) } != 0 {
        anyhow::bail!(
            "failed to stat the slot digit: {}",
            std::io::Error::last_os_error()
        );
    }
    let len = usize::try_from(stat.st_size).unwrap_or(0);
    let offset = (slot as usize).saturating_mul(std::mem::size_of::<AtomicU32>());
    if offset.saturating_add(std::mem::size_of::<AtomicU32>()) > len {
        anyhow::bail!("slot {slot} is outside the slot-digit page");
    }
    let map = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            held.0,
            0,
        )
    };
    if map == libc::MAP_FAILED {
        anyhow::bail!(
            "failed to map the slot digit: {}",
            std::io::Error::last_os_error()
        );
    }
    // The descriptor must not survive into a process Python starts. The
    // mapping stays for the life of this process; exit unmaps it.
    drop(held);
    // SAFETY: called once, before the runtime and the interpreter, so no other
    // thread is reading the environment.
    unsafe { std::env::remove_var("SIMPLE_Q_SLOT_FD") };
    let ptr = unsafe { (map as *mut u32).add(slot as usize) };
    if !ptr.is_aligned() {
        unsafe { libc::munmap(map, len) };
        anyhow::bail!("slot digit is not aligned");
    }
    let cell: &'static AtomicU32 = unsafe { AtomicU32::from_ptr(ptr) };
    if DIGIT.set(cell).is_err() {
        unsafe { libc::munmap(map, len) };
        anyhow::bail!("slot digit installed twice");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct CloseOnDrop(i32);

#[cfg(target_os = "linux")]
impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// The supervisor's one page. Drop unmaps it and closes the descriptor.
#[cfg(target_os = "linux")]
pub struct SlotDigits {
    fd: OwnedFd,
    map: *mut u8,
    len: usize,
    slots: u32,
}

// The page is written only with AtomicU32. The descriptor number does not change.
#[cfg(target_os = "linux")]
unsafe impl Send for SlotDigits {}
#[cfg(target_os = "linux")]
unsafe impl Sync for SlotDigits {}

#[cfg(target_os = "linux")]
impl SlotDigits {
    pub fn create(slots: u32) -> Result<Self> {
        let page = page_size();
        let bytes = (slots as usize)
            .saturating_mul(std::mem::size_of::<AtomicU32>())
            .max(std::mem::size_of::<AtomicU32>());
        let len = bytes.div_ceil(page) * page;
        let fd = unsafe {
            libc::memfd_create(
                b"simple-q-slot\0".as_ptr().cast::<libc::c_char>(),
                MFD_CLOEXEC | MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            anyhow::bail!("memfd_create failed: {}", std::io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
            anyhow::bail!(
                "ftruncate of the slot digit failed: {}",
                std::io::Error::last_os_error()
            );
        }
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if map == libc::MAP_FAILED {
            anyhow::bail!(
                "mmap of the slot digit failed: {}",
                std::io::Error::last_os_error()
            );
        }
        let seals = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, seals) } != 0 {
            unsafe { libc::munmap(map, len) };
            anyhow::bail!(
                "sealing the slot digit failed: {}",
                std::io::Error::last_os_error()
            );
        }
        let digits = Self {
            fd,
            map: map.cast::<u8>(),
            len,
            slots,
        };
        for slot in 0..slots {
            digits.reset(slot);
        }
        Ok(digits)
    }

    pub fn raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }

    #[cfg(test)]
    pub fn mapped_len(&self) -> usize {
        self.len
    }

    pub fn reset(&self, slot: u32) {
        self.with_cell(slot, |cell| cell.store(0, Ordering::Relaxed));
    }

    fn load(&self, slot: u32) -> u32 {
        self.with_cell(slot, |cell| cell.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn is_busy(&self, slot: u32) -> bool {
        self.load(slot) != 0
    }

    #[cfg(test)]
    pub fn fetch_add(&self, slot: u32, n: u32) -> Option<u32> {
        self.with_cell(slot, |cell| cell.fetch_add(n, Ordering::Relaxed))
    }

    #[cfg(test)]
    pub fn fetch_sub(&self, slot: u32, n: u32) -> Option<u32> {
        self.with_cell(slot, |cell| cell.fetch_sub(n, Ordering::Relaxed))
    }

    fn with_cell<T>(&self, slot: u32, f: impl FnOnce(&AtomicU32) -> T) -> Option<T> {
        if slot >= self.slots {
            return None;
        }
        let ptr = unsafe { (self.map as *mut u32).add(slot as usize) };
        if !ptr.is_aligned() {
            return None;
        }
        Some(f(unsafe { AtomicU32::from_ptr(ptr) }))
    }
}

#[cfg(target_os = "linux")]
impl Drop for SlotDigits {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.map.cast(), self.len) };
    }
}

#[cfg(target_os = "linux")]
fn page_size() -> usize {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page > 0 { page as usize } else { 4096 }
}

#[cfg(not(target_os = "linux"))]
pub struct SlotDigits;

#[cfg(not(target_os = "linux"))]
impl SlotDigits {
    pub fn raw_fd(&self) -> i32 {
        -1
    }

    pub fn reset(&self, _slot: u32) {}

    pub fn is_busy(&self, _slot: u32) -> bool {
        false
    }
}

/// Linux creates the page. Other systems have no digit, and a soft signal
/// then uses RSS only.
pub fn open(slots: u32) -> Result<Option<std::sync::Arc<SlotDigits>>> {
    #[cfg(target_os = "linux")]
    {
        Ok(Some(std::sync::Arc::new(SlotDigits::create(slots)?)))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = slots;
        Ok(None)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn one_page_is_sealed_shared_and_reset_before_the_next_child() {
        let digits = SlotDigits::create(2).unwrap();
        let fd = digits.raw_fd();
        let link = std::fs::read_link(format!("/proc/self/fd/{fd}")).unwrap();
        let name = link.to_string_lossy();
        assert!(name.contains("memfd:simple-q-slot"), "{name}");
        assert!(!name.contains("/dev/shm"), "{name}");

        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        assert_eq!(unsafe { libc::fstat(fd, &mut stat) }, 0);
        assert_eq!(stat.st_size as usize, digits.mapped_len());

        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
        clear_cloexec(fd).unwrap();
        let cleared = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_eq!(cleared & libc::FD_CLOEXEC, 0);
        unsafe { libc::fcntl(fd, libc::F_SETFD, cleared | libc::FD_CLOEXEC) };

        assert_eq!(unsafe { libc::ftruncate(fd, 0) }, -1);
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_ADD_SEALS, libc::F_SEAL_WRITE) },
            -1
        );

        let second = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                digits.mapped_len(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        assert_ne!(second, libc::MAP_FAILED);
        let cell = unsafe { AtomicU32::from_ptr(second as *mut u32) };
        assert_eq!(digits.fetch_add(0, 1), Some(0));
        assert_eq!(digits.fetch_add(0, 1), Some(1));
        assert_eq!(cell.load(Ordering::Relaxed), 2);
        assert_eq!(digits.fetch_sub(0, 1), Some(2));
        assert_eq!(cell.load(Ordering::Relaxed), 1);
        assert!(digits.is_busy(0));
        assert_eq!(digits.fetch_add(1, 1), Some(0));
        assert_eq!(cell.load(Ordering::Relaxed), 1);
        assert!(digits.is_busy(0));
        assert!(digits.is_busy(1));
        digits.reset(1);
        assert!(!digits.is_busy(1));
        digits.reset(0);
        assert_eq!(cell.load(Ordering::Relaxed), 0);
        assert!(!digits.is_busy(0));
        unsafe { libc::close(libc::dup(fd)) };
        assert_eq!(cell.load(Ordering::Relaxed), 0);
        unsafe { libc::munmap(second, digits.mapped_len()) };
    }
}
