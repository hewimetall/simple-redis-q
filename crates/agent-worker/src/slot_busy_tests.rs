use std::sync::atomic::Ordering;

use super::*;

#[test]
fn one_page_is_sealed_shared_and_reset_before_the_next_child() {
    // Arrange

    let digits = SlotDigits::create(2).unwrap();
    let fd = digits.raw_fd();
    let link = std::fs::read_link(format!("/proc/self/fd/{fd}")).unwrap();
    // Act

    let name = link.to_string_lossy();
    // Assert

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
