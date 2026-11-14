use super::*;

fn assert_class(
    signal: SentSignal,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
    state: ChildState,
    reason: Option<ChildReason>,
) {
    let (got_state, got_reason) = classify_child(signal, exit_code, term_signal);
    assert_eq!(got_state, state, "{signal:?} {exit_code:?} {term_signal:?}");
    assert_eq!(
        got_reason, reason,
        "{signal:?} {exit_code:?} {term_signal:?}"
    );
    assert_eq!(got_state.as_str(), state.as_str());
    assert_eq!(
        got_reason.map(ChildReason::as_str),
        reason.map(ChildReason::as_str)
    );
}

#[test]
fn classify_child_covers_every_exit_path() {
    // Arrange

    assert_class(SentSignal::None, None, None, ChildState::Running, None);
    assert_class(
        SentSignal::Usr1(DrainCause::MemorySoft),
        None,
        None,
        ChildState::Draining,
        Some(ChildReason::MemorySoft),
    );
    assert_class(
        SentSignal::Usr1(DrainCause::MemoryRatio),
        None,
        None,
        ChildState::Draining,
        Some(ChildReason::MemoryRatio),
    );
    assert_class(
        SentSignal::Usr1(DrainCause::Shutdown),
        None,
        None,
        ChildState::Draining,
        Some(ChildReason::Shutdown),
    );
    assert_class(
        SentSignal::Usr1(DrainCause::MemorySoft),
        Some(0),
        None,
        ChildState::Exited,
        Some(ChildReason::MemorySoft),
    );
    assert_class(
        SentSignal::Usr1(DrainCause::MemoryRatio),
        Some(0),
        None,
        ChildState::Exited,
        Some(ChildReason::MemoryRatio),
    );
    assert_class(
        SentSignal::Usr1(DrainCause::Shutdown),
        Some(0),
        None,
        ChildState::Exited,
        Some(ChildReason::Shutdown),
    );
    assert_class(
        SentSignal::None,
        Some(0),
        None,
        ChildState::Exited,
        Some(ChildReason::MaxTasks),
    );
    assert_class(
        SentSignal::Kill(KillCause::MemoryHard),
        None,
        None,
        ChildState::Killed,
        Some(ChildReason::MemoryHard),
    );
    assert_class(
        SentSignal::Kill(KillCause::MemoryRatio),
        None,
        None,
        ChildState::Killed,
        Some(ChildReason::MemoryRatio),
    );
    assert_class(
        SentSignal::Kill(KillCause::MemoryHard),
        None,
        Some(SIGKILL),
        ChildState::Killed,
        Some(ChildReason::MemoryHard),
    );
    assert_class(
        SentSignal::Kill(KillCause::MemoryRatio),
        None,
        Some(SIGKILL),
        ChildState::Killed,
        Some(ChildReason::MemoryRatio),
    );
    assert_class(
        SentSignal::None,
        Some(2),
        None,
        ChildState::Killed,
        Some(ChildReason::PingBudget),
    );
    assert_class(
        SentSignal::None,
        None,
        Some(SIGKILL),
        ChildState::Killed,
        Some(ChildReason::Sigkill),
    );
    assert_class(
        SentSignal::Usr1(DrainCause::MemorySoft),
        None,
        Some(SIGKILL),
        ChildState::Killed,
        Some(ChildReason::Sigkill),
    );
    assert_class(
        SentSignal::None,
        Some(1),
        None,
        ChildState::Killed,
        Some(ChildReason::Crash),
    ); // Act

    assert_class(
        SentSignal::None,
        None,
        Some(15),
        ChildState::Killed,
        Some(ChildReason::Crash),
    ); // Assert
}

#[test]
fn counters_follow_recycle_drain_and_kill() {
    // Arrange

    let mut recycled = ChildTrack::start(1);
    let mut counters = Counters::default();
    // Act

    recycled.apply_exit(Some(0), None, 10, &mut counters);
    // Assert

    assert_eq!(counters.recycled, 1);
    assert_eq!(counters.restarts, 0);
    assert_eq!(recycled.record().reason.as_deref(), Some("max_tasks"));

    let mut soft = ChildTrack::start(1);
    let mut counters = Counters::default();
    soft.apply_signal(SentSignal::Usr1(DrainCause::MemorySoft), &mut counters);
    assert_eq!(counters.drains, 1);
    assert_eq!(soft.record().state, "draining");
    assert_eq!(soft.record().reason.as_deref(), Some("memory_soft"));
    soft.apply_signal(SentSignal::Usr1(DrainCause::Shutdown), &mut counters);
    assert_eq!(counters.drains, 1);
    assert!(matches!(
        soft.signal,
        SentSignal::Usr1(DrainCause::MemorySoft)
    ));

    let mut shutdown = ChildTrack::start(1);
    let mut counters = Counters::default();
    shutdown.apply_signal(SentSignal::Usr1(DrainCause::Shutdown), &mut counters);
    assert_eq!(counters.drains, 0);
    assert_eq!(shutdown.record().reason.as_deref(), Some("shutdown"));

    for cause in [KillCause::MemoryHard, KillCause::MemoryRatio] {
        let mut track = ChildTrack::start(1);
        let mut counters = Counters::default();
        track.apply_signal(SentSignal::Kill(cause), &mut counters);
        assert_eq!(counters.kills, 1, "{cause:?}");
        assert_eq!(counters.restarts, 1, "{cause:?}");
        assert_eq!(track.record().state, "killed");
        assert!(track.ended_at.is_none());
        track.apply_exit(None, Some(SIGKILL), 20, &mut counters);
        assert_eq!(counters.kills, 1, "{cause:?}");
        assert_eq!(counters.restarts, 1, "{cause:?}");
        assert_eq!(track.record().state, "killed");
        assert_eq!(track.record().exit, Some(SIGKILL));
    }

    for (code, term, reason) in [
        (Some(2), None, "ping_budget"),
        (None, Some(SIGKILL), "sigkill"),
        (Some(1), None, "crash"),
        (None, Some(15), "crash"),
    ] {
        let mut track = ChildTrack::start(1);
        let mut counters = Counters::default();
        track.apply_exit(code, term, 5, &mut counters);
        assert_eq!(counters.restarts, 1, "{reason}");
        assert_eq!(counters.kills, 0, "{reason}");
        assert_eq!(counters.recycled, 0, "{reason}");
        assert_eq!(track.record().reason.as_deref(), Some(reason));
    }

    let mut drained = ChildTrack::start(1);
    let mut counters = Counters::default();
    drained.apply_signal(SentSignal::Usr1(DrainCause::MemorySoft), &mut counters);
    drained.apply_exit(Some(0), None, 30, &mut counters);
    assert_eq!(
        (
            counters.drains,
            counters.recycled,
            counters.kills,
            counters.restarts
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(drained.record().state, "exited");

    let mut both = ChildTrack::start(1);
    let mut counters = Counters::default();
    both.apply_signal(SentSignal::Usr1(DrainCause::MemoryRatio), &mut counters);
    both.apply_signal(SentSignal::Kill(KillCause::MemoryHard), &mut counters);
    assert_eq!(
        (
            counters.drains,
            counters.kills,
            counters.restarts,
            counters.recycled
        ),
        (1, 1, 1, 0)
    );
    both.apply_signal(SentSignal::Kill(KillCause::MemoryRatio), &mut counters);
    assert_eq!(counters.kills, 1);
    assert_eq!(both.record().reason.as_deref(), Some("memory_hard"));

    let mut once = ChildTrack::start(1);
    let mut counters = Counters::default();
    once.apply_exit(Some(0), None, 10, &mut counters);
    once.apply_exit(Some(1), None, 11, &mut counters);
    assert_eq!(counters.recycled, 1);
    assert_eq!(counters.restarts, 0);
    assert_eq!(once.ended_at, Some(10));
    assert_eq!(once.exit_code, Some(0));
}

#[test]
fn prune_finished_drops_a_row_at_exactly_two_windows() {
    // Arrange

    let window = 60u64;
    let retain = window * 2;
    // Act

    let now = 10_000u64;
    // Assert

    assert!(finished_row_expired(Some(now - retain), now, retain));
    assert!(!finished_row_expired(Some(now - retain + 1), now, retain));
    assert!(!finished_row_expired(None, now, retain));
    let mut children = vec![
        row(1, Some(now - retain)),
        row(2, Some(now - retain + 1)),
        row(3, None),
    ];
    prune_finished(&mut children, now, retain);
    let slots: Vec<u32> = children.iter().map(|child| child.slot).collect();
    assert_eq!(slots, vec![2, 3]);
    assert!(children.iter().any(|child| child.ended_at.is_none()));
}

fn row(slot: u32, ended_at: Option<u64>) -> ChildRecord {
    ChildRecord {
        pid: slot,
        slot,
        state: if ended_at.is_none() {
            "running".to_owned()
        } else {
            "exited".to_owned()
        },
        reason: None,
        rss: None,
        started_at: 1,
        ended_at,
        exit: None,
        tasks_done: None,
        max_tasks: None,
    }
}

#[test]
fn supervisor_keys_and_ttl_use_the_heartbeat_window() {
    // Arrange
    // Act
    // Assert

    assert_eq!(SUPERVISORS_KEY, "simple_q:supervisors");
    assert_eq!(supervisor_member("info-host", 12), "info-host.12");
    assert_eq!(
        supervisor_key("info-host.12"),
        "simple_q:supervisor:info-host.12"
    );
    assert_eq!(supervisor_member("info.host", 12), "info.host.12");
    let mut settings = Config::defaults();
    assert_eq!(supervisor_ttl_secs(&settings), 15 * 4 * 2);
    settings.heartbeat_secs = 2;
    settings.missed_heartbeats = 3;
    assert_eq!(supervisor_ttl_secs(&settings), 12);
    settings.heartbeat_secs = 0;
    settings.missed_heartbeats = 0;
    assert_eq!(supervisor_ttl_secs(&settings), 1);
}

#[test]
fn worker_and_supervisor_names_keep_a_dotted_host() {
    // Arrange

    let parsed = parse_worker_name("agent-worker.holdq.host.example.com.7.abcd1234").unwrap();
    // Act

    // Assert

    assert_eq!(parsed.host, "host.example.com");
    assert_eq!(parsed.pid, 7);
    assert!(parse_worker_name("agent-worker.holdq.host.7").is_none());
    assert_eq!(
        parse_supervisor_member("host.example.com.42").unwrap(),
        ("host.example.com".to_owned(), 42)
    );
    assert!(parse_supervisor_member(".7").is_none());
    assert!(parse_supervisor_member("nopid").is_none());
}

#[test]
fn child_record_json_omits_absent_fields_and_the_pid() {
    // Arrange

    let record = ChildTrack::start(3).record();
    // Act

    let json = serde_json::to_value(&record).unwrap();
    // Assert

    assert_eq!(json["slot"], 3);
    assert_eq!(json["state"], "running");
    assert_eq!(json["started_at"], 0);
    assert!(json.get("reason").is_none());
    assert!(json.get("rss").is_none());
    assert!(json.get("ended_at").is_none());
    assert!(json.get("exit").is_none());
    assert!(json.get("pid").is_none());
    assert!(json.get("tasks_done").is_none());
    assert!(json.get("max_tasks").is_none());
    assert_eq!(record.exit, None);
}

#[test]
fn an_exited_row_keeps_the_task_count_without_metadata() {
    // Arrange

    let mut child = running_child(12, 1);
    child.state = "exited".to_owned();
    child.reason = Some("max_tasks".to_owned());
    child.tasks_done = Some(500);
    child.max_tasks = Some(500);
    child.ended_at = Some(80);
    // Act

    let proc = ProcBuild::from_child("host", 12, 1, child);
    // Assert

    assert_eq!(tasks_done_of(&proc), Some(500));
    assert_eq!(max_tasks(&proc), Some(500));

    let mut existing = BTreeMap::new();
    existing.insert("tasks:12".to_owned(), "500".to_owned());
    let mut fresh = row(12, Some(80));
    restore_child_progress(&existing, &mut fresh, 500);
    assert_eq!(fresh.tasks_done, Some(500));
    assert_eq!(fresh.max_tasks, Some(500));
    existing.remove("tasks:12");
    existing.insert(
        "child:12".to_owned(),
        serde_json::to_string(&fresh).unwrap(),
    );
    let mut again = row(12, Some(80));
    restore_child_progress(&existing, &mut again, 500);
    assert_eq!(again.tasks_done, Some(500));
    assert_eq!(again.max_tasks, Some(500));
}

#[test]
fn a_missing_payload_still_yields_the_task_id() {
    // Arrange

    let missing = held_task_from_payload("holdq", "task-1", None, None, None, 0);
    // Act

    // Assert

    assert_eq!(missing.task_id, "task-1");
    assert_eq!(missing.actor_name, "");
    assert_eq!(missing.kwargs, serde_json::json!({}));
    assert!(missing.locked_at.is_none());

    let job = Job {
        version: crate::MESSAGE_VERSION,
        message_id: "m-1".to_owned(),
        actor_name: "recalc".to_owned(),
        queue: "holdq".to_owned(),
        kwargs: serde_json::json!({"project": "42"}),
        headers: Default::default(),
        retries: 1,
        max_retries: 2,
        time_limit_ms: 1_800_000,
        min_backoff_ms: 15_000,
        max_backoff_ms: 86_400_000,
        max_deliveries: 5,
        lock_key: None,
        lock_ttl: 30,
        skip_on_lock: false,
        unique_key: None,
    };
    let payload = serde_json::to_string(&job).unwrap();
    let task = held_task_from_payload(
        "holdq",
        "ulid-1",
        Some(&payload),
        Some("1700000000"),
        Some("1699999000"),
        4,
    );
    assert_eq!(task.task_id, "ulid-1");
    assert_eq!(task.message_id, "m-1");
    assert_eq!(task.actor_name, "recalc");
    assert_eq!(task.kwargs, serde_json::json!({"project": "42"}));
    assert_eq!(task.retries, 1);
    assert_eq!(task.locked_at, Some(1_700_000_000));
    assert_eq!(task.started_at, Some(1_699_999_000));
    assert_eq!(task.deliveries, 4);
}

fn worker(name: &str, score: f64, meta: &[(&str, &str)], inflight: &[&str]) -> LoadedWorker {
    LoadedWorker {
        name: name.to_owned(),
        score,
        meta: meta
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        inflight: inflight.iter().map(|id| (*id).to_owned()).collect(),
    }
}

fn running_child(pid: u32, slot: u32) -> ChildRecord {
    let mut child = ChildTrack::start(slot).record();
    child.pid = pid;
    child.started_at = 50;
    child
}

#[test]
fn the_tree_joins_children_orphans_and_queue_counts() {
    // Arrange

    let now = 1_000u64;
    let window = 60u64;
    let host = "host.example.com";
    let worker_name = "agent-worker.holdq.host.example.com.7.abcd1234";
    let mut child = running_child(7, 0);
    child.rss = Some(10);
    let child_json = serde_json::to_string(&child).unwrap();
    let mut fields = BTreeMap::new();
    fields.insert("version".to_owned(), "1.0.0-rc1".to_owned());
    fields.insert("queues".to_owned(), "holdq".to_owned());
    fields.insert("processes".to_owned(), "1".to_owned());
    fields.insert("concurrency".to_owned(), "2".to_owned());
    fields.insert("max_tasks".to_owned(), "500".to_owned());
    fields.insert("child_soft".to_owned(), String::new());
    fields.insert("started_at".to_owned(), "40".to_owned());
    fields.insert(format!("child:{pid}", pid = 7), child_json);
    let mut finished = running_child(8, 1);
    finished.state = "exited".to_owned();
    finished.ended_at = Some(900);
    finished.reason = Some("max_tasks".to_owned());
    fields.insert(
        "child:8".to_owned(),
        serde_json::to_string(&finished).unwrap(),
    );
    let queues = vec![QueueDraft {
        name: "holdq".to_owned(),
        active: 1,
        scheduled: 0,
        done: 0,
        dead: 0,
        counted: true,
        workers: vec![
            worker(
                worker_name,
                now as f64,
                &[
                    ("ppid", "3"),
                    ("slot", "0"),
                    ("started_at", "51"),
                    ("max_tasks", "500"),
                    ("tasks_done", "2"),
                ],
                &["b-task", "a-task"],
            ),
            worker(
                "agent-worker.holdq.other-host.9.abcd1234",
                now as f64,
                &[("ppid", "1")],
                &["hidden-1", "hidden-2"],
            ),
        ],
    }];
    let held = BTreeMap::from([
        (
            ("holdq".to_owned(), "a-task".to_owned()),
            HeldTask {
                queue: "holdq".to_owned(),
                task_id: "a-task".to_owned(),
                message_id: "m-a".to_owned(),
                actor_name: "recalc".to_owned(),
                kwargs: serde_json::json!({"project": "42"}),
                retries: 1,
                max_retries: 2,
                time_limit_ms: 30,
                locked_at: Some(90),
                started_at: Some(80),
                deliveries: 3,
            },
        ),
        (
            ("holdq".to_owned(), "b-task".to_owned()),
            held_task_from_payload("holdq", "b-task", None, None, None, 0),
        ),
    ]);
    // Act

    let snap = build_snapshot(
        now,
        window,
        Some(host),
        queues,
        vec![LoadedSup {
            host: host.to_owned(),
            pid: 3,
            score: now as f64,
            fields,
        }],
        &held,
    );
    // Assert

    assert_eq!(snap.taken_at, now);
    assert_eq!(snap.queues[0].inflight, 4);
    assert_eq!(snap.queues[0].workers, 2);
    assert_eq!(snap.queues[0].workers_alive, 2);
    assert_eq!(snap.supervisors.len(), 1);
    assert!(snap.supervisors[0].child_soft.is_none());
    assert_eq!(snap.supervisors[0].queues, vec!["holdq".to_owned()]);
    assert_eq!(snap.supervisors[0].children.len(), 2);
    let live = &snap.supervisors[0].children[0];
    assert!(live.ended_at.is_none());
    assert_eq!(live.pid, 7);
    assert_eq!(live.slot, Some(0));
    assert_eq!(live.state, "running");
    assert_eq!(live.ppid, Some(3));
    assert_eq!(live.workers, vec![worker_name.to_owned()]);
    assert_eq!(live.tasks.len(), 2);
    assert_eq!(live.tasks[0].task_id, "a-task");
    assert_eq!(live.tasks[0].actor_name, "recalc");
    assert_eq!(live.tasks[1].task_id, "b-task");
    assert_eq!(live.tasks[1].actor_name, "");
    let done = &snap.supervisors[0].children[1];
    assert_eq!(done.pid, 8);
    assert_eq!(done.state, "exited");
    assert!(done.workers.is_empty());
    assert!(snap.orphans.is_empty());
}

#[test]
fn orphans_follow_the_metadata_hash_and_the_heartbeat() {
    // Arrange

    let now = 1_000u64;
    let window = 60u64;
    let host = "info-host";
    let queues = vec![QueueDraft {
        name: "holdq".to_owned(),
        active: 0,
        scheduled: 0,
        done: 0,
        dead: 0,
        counted: true,
        workers: vec![
            worker(
                "agent-worker.holdq.info-host.11.stale001",
                (now - window - 1) as f64,
                &[("ppid", "99"), ("tasks_done", "1")],
                &[],
            ),
            worker(
                "agent-worker.holdq.info-host.12.nometa01",
                now as f64,
                &[],
                &[],
            ),
            worker(
                "agent-worker.holdq.info-host.13.fresh001",
                (now - window) as f64,
                &[("ppid", "99")],
                &[],
            ),
        ],
    }];
    // Act

    let snap = build_snapshot(
        now,
        window,
        Some(host),
        queues,
        Vec::new(),
        &BTreeMap::new(),
    );
    // Assert

    assert!(snap.supervisors.is_empty());
    assert_eq!(snap.queues[0].workers_alive, 2);
    let states: Vec<(&str, u32, &str, bool)> = snap
        .orphans
        .iter()
        .map(|proc| {
            (
                proc.state.as_str(),
                proc.pid,
                proc.reason.as_deref().unwrap_or(""),
                proc.alive,
            )
        })
        .collect();
    assert_eq!(
        states,
        vec![
            ("killed", 11, "", false),
            ("exited", 12, "", true),
            ("running", 13, "", true),
        ]
    );
}

#[test]
fn a_running_child_at_max_tasks_is_shown_as_draining() {
    // Arrange

    let now = 1_000u64;
    let child = running_child(7, 0);
    let mut fields = BTreeMap::new();
    fields.insert("child:7".to_owned(), serde_json::to_string(&child).unwrap());
    let queues = vec![QueueDraft {
        name: "holdq".to_owned(),
        active: 0,
        scheduled: 0,
        done: 0,
        dead: 0,
        counted: true,
        workers: vec![worker(
            "agent-worker.holdq.info-host.7.abcd1234",
            now as f64,
            &[("ppid", "3"), ("tasks_done", "5"), ("max_tasks", "5")],
            &[],
        )],
    }];
    let snap = build_snapshot(
        now,
        60,
        None,
        queues,
        vec![LoadedSup {
            host: "info-host".to_owned(),
            pid: 3,
            score: now as f64,
            fields,
        }],
        &BTreeMap::new(),
    );
    // Act

    let shown = &snap.supervisors[0].children[0];
    // Assert

    assert_eq!(shown.state, "draining");
    assert_eq!(shown.reason.as_deref(), Some("max_tasks"));
    assert_eq!(shown.tasks_done, Some(5));
    assert_eq!(shown.max_tasks, Some(5));
}

#[test]
fn an_uncounted_queue_adds_tasks_without_a_counter_row() {
    // Arrange

    let now = 1_000u64;
    let quota_worker = "agent-worker.quota.info-host.7.abcd1234";
    let default_worker = "agent-worker.default.info-host.7.abcd1234";
    let queues = vec![
        QueueDraft {
            name: "quota".to_owned(),
            active: 2,
            scheduled: 0,
            done: 0,
            dead: 0,
            counted: true,
            workers: vec![worker(
                quota_worker,
                now as f64,
                &[("ppid", "3")],
                &["quota-task"],
            )],
        },
        QueueDraft {
            name: "default".to_owned(),
            active: 9,
            scheduled: 4,
            done: 1,
            dead: 1,
            counted: false,
            workers: vec![worker(
                default_worker,
                now as f64,
                &[("ppid", "3")],
                &["default-task"],
            )],
        },
    ];
    let mut fields = BTreeMap::new();
    fields.insert("queues".to_owned(), "quota,default".to_owned());
    let mut child = running_child(7, 0);
    child.pid = 7;
    fields.insert("child:7".to_owned(), serde_json::to_string(&child).unwrap());
    let held = BTreeMap::from([
        (
            ("quota".to_owned(), "quota-task".to_owned()),
            held_task_from_payload("quota", "quota-task", None, None, None, 0),
        ),
        (
            ("default".to_owned(), "default-task".to_owned()),
            held_task_from_payload("default", "default-task", None, None, None, 0),
        ),
    ]);
    // Act

    let snap = build_snapshot(
        now,
        60,
        None,
        queues,
        vec![LoadedSup {
            host: "info-host".to_owned(),
            pid: 3,
            score: now as f64,
            fields,
        }],
        &held,
    );
    // Assert

    assert_eq!(snap.queues.len(), 1);
    assert_eq!(snap.queues[0].name, "quota");
    assert_eq!(snap.queues[0].active, 2);
    assert_eq!(snap.queues[0].inflight, 1);
    let tasks: Vec<&str> = snap.supervisors[0].children[0]
        .tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    assert_eq!(tasks, vec!["default-task", "quota-task"]);
    assert_eq!(
        snap.supervisors[0].children[0].workers,
        vec![default_worker.to_owned(), quota_worker.to_owned()]
    );
}
