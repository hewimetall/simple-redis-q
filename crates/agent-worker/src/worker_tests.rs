use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use apalis::prelude::WorkerError;

use super::join_tracked;
use super::restart_after_backend_error;

#[tokio::test]
async fn shutdown_waits_for_an_idempotency_release() {
    // Arrange

    let released = Arc::new(AtomicBool::new(false));
    let flag = released.clone();
    let handles = Mutex::new(vec![tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        flag.store(true, Ordering::SeqCst);
    })]);
    // Act

    join_tracked(&handles).await;
    // Assert

    assert!(released.load(Ordering::SeqCst));
    assert!(handles.lock().expect("releases").is_empty());
}

#[test]
fn a_backend_error_restarts_and_a_panic_does_not() {
    // Arrange

    let backend = WorkerError::BackendError(Box::new(std::io::Error::other("broken pipe")));
    // Act

    // Assert

    assert!(restart_after_backend_error(0, &backend));
    assert!(restart_after_backend_error(7, &backend));
    assert!(!restart_after_backend_error(8, &backend));
    let panic = WorkerError::PanicError("handler".to_owned());
    assert!(!restart_after_backend_error(0, &panic));
}
