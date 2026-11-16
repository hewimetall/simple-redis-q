use super::*;

#[test]
fn lock_owners_are_distinct_uuids_not_pids() {
    // Arrange

    let first = lock_owner();
    // Act

    let second = lock_owner();
    // Assert

    assert_ne!(first, second);
    assert!(Uuid::parse_str(&first).is_ok());
    assert_ne!(first, process::id().to_string());
}

#[test]
fn worker_names_include_host_pid_and_a_random_suffix() {
    // Arrange

    let queue = "default";
    let first = worker_name(queue);
    // Act

    let second = worker_name(queue);
    // Assert

    assert_ne!(first, second);
    assert!(first.starts_with(&format!(
        "{}.{queue}.{}.{}",
        crate::names::WORKER_NAME_PREFIX,
        hostname(),
        process::id()
    )));
}
