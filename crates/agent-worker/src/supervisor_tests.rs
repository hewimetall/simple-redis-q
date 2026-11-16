use super::*;

#[tokio::test]
async fn a_sigterm_before_the_loop_is_still_waiting() {
    // Arrange

    let mut signals = arm_stop_signals().await.expect("signal handler");
    unsafe { libc::raise(libc::SIGTERM) };
    let mut seen = false;
    // Act

    for _ in 0..20 {
        tokio::task::yield_now().await;
        if stop_requested(&mut signals).await {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Assert

    assert!(seen, "SIGTERM was dropped before the supervisor loop");
}

#[test]
fn an_exit_before_the_first_heartbeat_shares_one_redis_clock() {
    // Arrange

    let mut child = ChildTrack::start(0);
    let mut counters = Counters::default();
    child.apply_exit(Some(0), None, 0, &mut counters);
    let redis_now = 1_700_000_000;
    // Act

    stamp_child_clocks(&mut child, redis_now);
    // Assert

    assert_eq!(child.started_at, redis_now);
    assert_eq!(child.ended_at, Some(redis_now));
    assert!(!simple_q_runtime::admin::finished_row_expired(
        child.ended_at,
        redis_now,
        40
    ));
}

#[test]
fn exit_code_3_is_fatal_and_other_codes_restart() {
    // Arrange
    // Act
    // Assert

    assert_eq!(classify_child_exit(true, Some(0)), ChildExit::Recycle);
    assert_eq!(classify_child_exit(false, Some(3)), ChildExit::Fatal);
    assert_eq!(classify_child_exit(false, Some(1)), ChildExit::Restart);
    assert_eq!(classify_child_exit(false, Some(2)), ChildExit::Restart);
    assert_eq!(classify_child_exit(false, None), ChildExit::Restart);
}

#[tokio::test]
async fn a_panicked_slot_fails_supervisor_shutdown() {
    // Arrange

    let joined = tokio::spawn(async {
        panic!("slot");
    })
    .await;
    let mut fatal = None;
    note_shutdown_join(joined, &mut fatal);
    // Act

    let error = fatal.expect("a panicked slot was treated as success");
    // Assert

    assert!(error.to_string().contains("panicked"));
}

#[test]
fn the_current_process_reports_a_resident_size() {
    // Arrange

    let rss = resident_bytes(std::process::id()).expect("resident size");
    // Act

    // Assert

    assert!(rss > 0);
}

#[test]
fn memory_thresholds_drain_then_kill_and_ignore_an_unlimited_cgroup() {
    // Arrange
    // Act
    // Assert

    assert_eq!(memory_action(70, 100, 0.80, 0.95), MemoryAction::None);
    assert_eq!(memory_action(80, 100, 0.80, 0.95), MemoryAction::Drain);
    assert_eq!(memory_action(95, 100, 0.80, 0.95), MemoryAction::Kill);
    assert_eq!(memory_action(90, 100, 0.99, 0.90), MemoryAction::Kill);
    assert_eq!(memory_action(70, 100, 0.99, 0.90), MemoryAction::None);
    assert_eq!(memory_action(10, 0, 0.80, 0.95), MemoryAction::None);
    assert_eq!(memory_action(10, 100, 0.0, 0.95), MemoryAction::None);
    assert_eq!(parse_memory_max("max"), None);
    assert_eq!(parse_memory_max("  "), None);
    assert_eq!(parse_memory_max("1073741824"), Some(1_073_741_824));
    assert_eq!(
        cgroup_memory_dir("0::/kubepods/pod\n"),
        Some(std::path::PathBuf::from("/sys/fs/cgroup/kubepods/pod"))
    );
    assert_eq!(cgroup_memory_dir("1:name=systemd:/\n"), None);
}

#[test]
fn only_a_child_that_owns_the_usage_is_killed() {
    // Arrange

    let children = [(4, 10_u64), (9, 80)];
    // Act

    // Assert

    assert_eq!(largest_child_to_kill(&children, 100), Some(9));
    assert_eq!(largest_child_to_kill(&[(4, 10)], 100), None);
    assert_eq!(largest_child_to_kill(&[], 100), None);
}

#[test]
fn byte_sizes_accept_binary_units_and_reject_zero_junk_fractions_and_soft_above_hard() {
    // Arrange
    // Act
    // Assert

    assert_eq!(parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
    assert_eq!(parse_byte_size("1500M").unwrap(), 1500 * 1024 * 1024);
    assert_eq!(parse_byte_size("512K").unwrap(), 512 * 1024);
    assert_eq!(parse_byte_size("4096").unwrap(), 4096);
    assert!(parse_byte_size("0").is_err());
    assert!(parse_byte_size("0M").is_err());
    assert!(parse_byte_size("abc").is_err());
    assert!(parse_byte_size("1.5G").is_err());
    assert!(parse_byte_size("1.5").is_err());
    assert!(load_child_memory_limits(None, None, Some("1G"), Some("1G")).is_err());
    assert!(load_child_memory_limits(None, None, Some("2G"), Some("1G")).is_err());
    assert!(load_child_memory_limits(None, None, Some("1500M"), Some("1G")).is_err());
    assert!(load_child_memory_limits(None, None, Some("1024K"), Some("1M")).is_err());
    let limits = load_child_memory_limits(None, None, Some("1G"), Some("1500M")).unwrap();
    assert!(limits.soft.unwrap() < limits.hard.unwrap());
    let soft_only = load_child_memory_limits(None, None, Some("512K"), None).unwrap();
    assert_eq!(soft_only.soft, Some(512 * 1024));
    assert_eq!(soft_only.hard, None);
    let hard_only = load_child_memory_limits(None, None, None, Some("1G")).unwrap();
    assert_eq!(hard_only.soft, None);
    assert_eq!(hard_only.hard, Some(1024 * 1024 * 1024));
    assert_eq!(
        load_child_memory_limits(None, None, None, None).unwrap(),
        ChildMemoryLimits {
            soft: None,
            hard: None,
        }
    );
    let zero = parse_byte_size("0").unwrap_err();
    assert!(zero.to_string().contains("greater than zero"));
    let junk = parse_byte_size("abc").unwrap_err();
    assert!(junk.to_string().contains("invalid"));
    let fraction = parse_byte_size("1.5G").unwrap_err();
    assert!(fraction.to_string().contains("invalid"));
    let order = load_child_memory_limits(None, None, Some("2G"), Some("1G")).unwrap_err();
    assert!(order.to_string().contains("not below"));
}

#[test]
fn the_flag_wins_over_the_configured_limit() {
    // Arrange

    let limits = load_child_memory_limits(Some(1), Some(2), Some("512K"), Some("1G")).unwrap();
    // Act

    // Assert

    assert_eq!(limits.soft, Some(512 * 1024));
    assert_eq!(limits.hard, Some(1024 * 1024 * 1024));
    let configured =
        load_child_memory_limits(Some(512 * 1024), Some(1024 * 1024 * 1024), None, None).unwrap();
    assert_eq!(configured.soft, Some(512 * 1024));
    assert_eq!(configured.hard, Some(1024 * 1024 * 1024));
    assert!(load_child_memory_limits(Some(2_000), Some(1_000), None, None).is_err());
}

#[test]
fn byte_action_is_per_pid() {
    // Arrange
    // Act
    // Assert

    assert_eq!(
        byte_action(8_000, Some(1_500), Some(8_000)),
        ByteAction::Kill
    );
    assert_eq!(
        byte_action(1_500, Some(1_500), Some(8_000)),
        ByteAction::Drain
    );
    assert_eq!(
        byte_action(1_499, Some(1_500), Some(8_000)),
        ByteAction::None
    );
    assert_eq!(byte_action(9_000, None, None), ByteAction::None);
    assert_eq!(byte_action(9_000, Some(1_000), None), ByteAction::Drain);
    assert_eq!(byte_action(9_000, None, Some(8_000)), ByteAction::Kill);
}

#[test]
fn rss_selection_when_no_slot_is_busy() {
    // Arrange

    let children = [(11_u32, 100_u64), (22, 2_000), (33, 9_000)];
    let soft = 0.80;
    let hard = 0.95;
    // Act

    let over = plan_memory_signals(
        &children,
        Some(1_500),
        Some(8_000),
        None,
        None,
        soft,
        hard,
        &[],
        &[],
    );
    // Assert

    assert_eq!(
        over,
        vec![
            PlannedSignal::Drain(22, DrainCause::MemorySoft),
            PlannedSignal::Kill(33, KillCause::MemoryHard),
        ]
    );

    let first = plan_memory_signals(
        &children,
        None,
        None,
        Some(80),
        Some(100),
        soft,
        hard,
        &[],
        &[],
    );
    assert_eq!(
        first,
        vec![PlannedSignal::Drain(33, DrainCause::MemoryRatio)]
    );
    let second = plan_memory_signals(
        &children,
        None,
        None,
        Some(80),
        Some(100),
        soft,
        hard,
        &[33],
        &[],
    );
    assert_eq!(
        second,
        vec![PlannedSignal::Drain(22, DrainCause::MemoryRatio)]
    );
    let third = plan_memory_signals(
        &children,
        None,
        None,
        Some(80),
        Some(100),
        soft,
        hard,
        &[33, 22],
        &[],
    );
    assert_eq!(
        third,
        vec![PlannedSignal::Drain(11, DrainCause::MemoryRatio)]
    );
    assert!(
        plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[33, 22, 11],
            &[],
        )
        .is_empty()
    );

    let owned = plan_memory_signals(
        &[(4, 10), (9, 80)],
        None,
        None,
        Some(100),
        Some(100),
        soft,
        hard,
        &[],
        &[],
    );
    assert_eq!(owned, vec![PlannedSignal::Kill(9, KillCause::MemoryRatio)]);
    let shared = plan_memory_signals(
        &[(4, 10), (9, 40)],
        None,
        None,
        Some(100),
        Some(100),
        soft,
        hard,
        &[],
        &[],
    );
    assert_eq!(
        shared,
        vec![PlannedSignal::Drain(9, DrainCause::MemoryRatio)]
    );
    assert_eq!(
        plan_memory_signals(
            &[(4, 10)],
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[4],
            &[],
        ),
        Vec::<PlannedSignal>::new()
    );

    let both_large = [(1_u32, 60_u64), (2, 55)];
    assert_eq!(
        plan_memory_signals(
            &both_large,
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[],
        ),
        vec![PlannedSignal::Kill(1, KillCause::MemoryRatio)]
    );
    assert_eq!(
        plan_memory_signals(
            &[(1, 80), (2, 60)],
            None,
            Some(70),
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[],
        ),
        vec![PlannedSignal::Kill(1, KillCause::MemoryHard)]
    );

    let already_soft = plan_memory_signals(
        &children,
        Some(1_500),
        None,
        Some(80),
        Some(100),
        soft,
        hard,
        &[],
        &[],
    );
    assert_eq!(
        already_soft,
        vec![
            PlannedSignal::Drain(22, DrainCause::MemorySoft),
            PlannedSignal::Drain(33, DrainCause::MemorySoft),
        ]
    );
}

#[test]
fn soft_signals_skip_a_busy_child() {
    // Arrange

    let children = [(11_u32, 100_u64), (22, 2_000), (33, 9_000)];
    let soft = 0.80;
    // Act

    let hard = 0.95;
    // Assert

    assert_eq!(
        plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[33],
        ),
        vec![PlannedSignal::Drain(22, DrainCause::MemoryRatio)]
    );
    assert_eq!(
        plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[33, 22],
        ),
        vec![PlannedSignal::Drain(11, DrainCause::MemoryRatio)]
    );
    assert!(
        plan_memory_signals(
            &children,
            None,
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[33, 22, 11],
        )
        .is_empty()
    );

    assert_eq!(
        plan_memory_signals(
            &children,
            Some(1_500),
            None,
            None,
            None,
            soft,
            hard,
            &[],
            &[33],
        ),
        vec![PlannedSignal::Drain(22, DrainCause::MemorySoft)]
    );
    assert_eq!(
        plan_memory_signals(
            &children,
            Some(1_500),
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[33],
        ),
        vec![PlannedSignal::Drain(22, DrainCause::MemorySoft)]
    );
    assert_eq!(
        plan_memory_signals(
            &children,
            Some(1_500),
            None,
            Some(80),
            Some(100),
            soft,
            hard,
            &[],
            &[33, 22],
        ),
        vec![PlannedSignal::Drain(11, DrainCause::MemoryRatio)]
    );

    assert_eq!(
        plan_memory_signals(
            &children,
            Some(1_500),
            Some(8_000),
            None,
            None,
            soft,
            hard,
            &[],
            &[33],
        ),
        vec![
            PlannedSignal::Drain(22, DrainCause::MemorySoft),
            PlannedSignal::Kill(33, KillCause::MemoryHard),
        ]
    );
    assert_eq!(
        plan_memory_signals(
            &[(9, 80)],
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[9],
        ),
        vec![PlannedSignal::Kill(9, KillCause::MemoryRatio)]
    );
    assert_eq!(
        plan_memory_signals(
            &[(9, 80), (4, 30)],
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[9],
        ),
        vec![PlannedSignal::Kill(9, KillCause::MemoryRatio)]
    );
    assert_eq!(
        plan_memory_signals(
            &[(9, 40), (4, 30)],
            None,
            None,
            Some(100),
            Some(100),
            soft,
            hard,
            &[],
            &[9],
        ),
        vec![PlannedSignal::Drain(4, DrainCause::MemoryRatio)]
    );
}

#[test]
fn working_set_subtracts_inactive_file_before_the_ratio() {
    // Arrange

    let stat = "anon 90\ninactive_file 20\nfile 40\n";
    // Act

    // Assert

    assert_eq!(inactive_file_bytes(stat), Some(20));
    assert_eq!(inactive_file_bytes("anon 90\n"), None);
    assert_eq!(working_set(100, Some(20)), 80);
    assert_eq!(working_set(100, None), 100);
    assert_eq!(working_set(10, Some(40)), 0);
    let usage = working_set(100, inactive_file_bytes(stat));
    assert_eq!(memory_action(usage, 100, 0.80, 0.95), MemoryAction::Drain);
    assert_eq!(memory_action(100, 100, 0.80, 0.95), MemoryAction::Kill);
}

#[test]
fn a_hard_budget_above_the_cgroup_is_only_a_warning_condition() {
    // Arrange
    // Act
    // Assert

    assert!(hard_budget_exceeds_cgroup(2, 100, 150));
    assert!(!hard_budget_exceeds_cgroup(2, 100, 200));
    assert!(hard_budget_exceeds_cgroup(2, 100, 199));
}

#[test]
fn child_memory_prom_file_rewrites_both_counters() {
    // Arrange

    let dir = std::env::temp_dir().join(format!("simple-q-child-memory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write_child_memory_prom(&dir, 3, 1).unwrap();
    // Act

    let text = std::fs::read_to_string(dir.join("simple_q_child_memory.prom")).unwrap();
    // Assert

    assert!(text.contains("simple_q_child_memory_drains_total 3\n"));
    assert!(text.contains("simple_q_child_memory_kills_total 1\n"));
    write_child_memory_prom(&dir, 4, 2).unwrap();
    let text = std::fs::read_to_string(dir.join("simple_q_child_memory.prom")).unwrap();
    assert!(text.contains("simple_q_child_memory_drains_total 4\n"));
    assert!(text.contains("simple_q_child_memory_kills_total 2\n"));
    assert!(!text.contains("simple_q_child_memory_drains_total 3\n"));
    let _ = std::fs::remove_dir_all(&dir);
}
