use super::*;

#[test]
fn done_records_expire_on_their_ttl_and_fresh_dead_records_stay() {
    // Arrange

    let now = 1_700_000_000i64;
    let day = 24 * 60 * 60u64;
    // Act

    let week = 7 * day;
    // Assert

    assert!(should_delete(now - day as i64, now, day));
    assert!(!should_delete(now - day as i64 + 1, now, day));
    assert!(!should_delete(now - day as i64, now, week));
    assert!(should_delete(now - week as i64, now, week));
}

#[test]
fn metadata_absent_from_the_member_set_is_deleted_and_a_present_member_is_kept() {
    // Arrange

    let members = BTreeSet::from(["agent-worker.default.host.1.live"]);
    // Act

    // Assert

    assert!(should_delete_worker_metadata(
        "agent-worker.default.host.1.gone",
        &members
    ));
    assert!(!should_delete_worker_metadata(
        "agent-worker.default.host.1.live",
        &members
    ));
}
