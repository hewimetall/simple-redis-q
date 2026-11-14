use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::request_drain_once;

#[test]
fn drain_is_requested_once() {
    // Arrange

    let shutdown = AtomicBool::new(false);
    let pauses = AtomicUsize::new(0);
    // Act

    let pause = || {
        pauses.fetch_add(1, Ordering::SeqCst);
    };
    // Assert

    assert!(request_drain_once(&shutdown, pause));
    assert!(!request_drain_once(&shutdown, pause));
    assert!(shutdown.load(Ordering::SeqCst));
    assert_eq!(pauses.load(Ordering::SeqCst), 1);
}
