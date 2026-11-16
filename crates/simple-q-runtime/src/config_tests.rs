use std::sync::Mutex;
use std::time::Duration;

use super::*;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    name: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var(name).ok();
        // Safety: tests take ENV_LOCK so only one config test mutates these variables.
        unsafe { std::env::set_var(name, value) };
        Self { name, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe { std::env::set_var(self.name, value) },
            None => unsafe { std::env::remove_var(self.name) },
        }
    }
}

#[test]
fn absent_variables_keep_the_defaults() {
    // Arrange

    let _guard = ENV_LOCK.lock().unwrap();
    let names = [
        "SIMPLE_Q_HEARTBEAT_SECS",
        "SIMPLE_Q_MISSED_HEARTBEATS",
        "SIMPLE_Q_POLL_INTERVAL_MS",
        "SIMPLE_Q_PING_INTERVAL_MS",
        "SIMPLE_Q_PING_TIMEOUT_MS",
        "SIMPLE_Q_LIFECYCLE_INTERVAL_MS",
        "SIMPLE_Q_SWEEP_INTERVAL_SECS",
        "SIMPLE_Q_SWEEP_BATCH",
        "SIMPLE_Q_BATCH_SIZE",
        "SIMPLE_Q_IDEMPOTENCY_WAIT_MS",
        "SIMPLE_Q_IDEMPOTENCY_WAIT_MAX_MS",
        "SIMPLE_Q_PING_BUDGET_SECS",
        "SIMPLE_Q_DONE_TTL_SECS",
        "SIMPLE_Q_DEAD_TTL_SECS",
        "SIMPLE_Q_UNIQUE_TTL_SECS",
        "SIMPLE_Q_MAX_DELIVERIES",
        "SIMPLE_Q_MEMORY_SOFT_RATIO",
        "SIMPLE_Q_MEMORY_HARD_RATIO",
        "SIMPLE_Q_CHILD_MEMORY_SOFT",
        "SIMPLE_Q_CHILD_MEMORY_HARD",
    ];
    let saved: Vec<_> = names
        .iter()
        .map(|name| (*name, std::env::var(name).ok()))
        .collect();
    for name in names {
        unsafe { std::env::remove_var(name) };
    }
    // Act

    let config = Config::from_env().unwrap();
    // Assert

    assert_eq!(config, Config::defaults());
    assert_eq!(config.heartbeat_secs, 15);
    assert_eq!(config.missed_heartbeats, 4);
    assert_eq!(config.poll_interval_ms, 500);
    assert_eq!(config.ping_interval_ms, 2_000);
    assert_eq!(config.ping_timeout_ms, 3_000);
    assert_eq!(config.lifecycle_interval_ms, 200);
    assert_eq!(config.sweep_interval_secs, 60);
    assert_eq!(config.sweep_batch, 100);
    assert_eq!(config.batch_size, 1);
    assert_eq!(config.idempotency_wait_ms, 20);
    assert_eq!(config.idempotency_wait_max_ms, 5_000);
    assert_eq!(config.ping_budget_secs, 1_860);
    assert_eq!(config.unique_ttl_secs, 86_400);
    assert!(config.unique_ttl_secs > 0);
    assert!(config.memory_soft_ratio < config.memory_hard_ratio);
    for (name, previous) in saved {
        match previous {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }
}

#[test]
fn an_unparsed_heartbeat_and_an_out_of_range_ratio_fail() {
    // Arrange

    let _guard = ENV_LOCK.lock().unwrap();
    {
        let _heartbeat = EnvGuard::set("SIMPLE_Q_HEARTBEAT_SECS", "abc");
        let error = Config::from_env().unwrap_err().to_string();
        assert!(error.contains("SIMPLE_Q_HEARTBEAT_SECS"), "{error}");
    }
    {
        let _poll = EnvGuard::set("SIMPLE_Q_POLL_INTERVAL_MS", "0");
        let error = Config::from_env().unwrap_err().to_string();
        assert!(error.contains("SIMPLE_Q_POLL_INTERVAL_MS"), "{error}");
    }
    {
        let _wait = EnvGuard::set("SIMPLE_Q_IDEMPOTENCY_WAIT_MS", "1000");
        let _cap = EnvGuard::set("SIMPLE_Q_IDEMPOTENCY_WAIT_MAX_MS", "100");
        let error = Config::from_env().unwrap_err().to_string();
        assert!(
            error.contains("SIMPLE_Q_IDEMPOTENCY_WAIT_MAX_MS"),
            "{error}"
        );
    } // Act

    {
        let _ratio = EnvGuard::set("SIMPLE_Q_MEMORY_SOFT_RATIO", "1.5");
        let error = Config::from_env().unwrap_err().to_string();
        assert!(error.contains("SIMPLE_Q_MEMORY_SOFT_RATIO"), "{error}");
        assert!(error.contains("(0, 1)"), "{error}");
    } // Assert
}

#[test]
fn zero_is_rejected_where_a_positive_value_is_required() {
    // Arrange

    let _guard = ENV_LOCK.lock().unwrap();
    let _ttl = EnvGuard::set("SIMPLE_Q_UNIQUE_TTL_SECS", "0");
    // Act

    let error = Config::from_env().unwrap_err().to_string();
    // Assert

    assert!(error.contains("greater than zero"), "{error}");
}

#[test]
fn given_defaults_when_durations_are_read_then_they_match_the_stored_numbers() {
    // Arrange
    let config = Config::defaults();

    // Act
    let window = config.orphan_window_secs();
    let budget = config.ping_budget();
    let poll = config.poll_interval();
    let ping = config.ping_interval();
    let timeout = config.ping_timeout();
    let lifecycle = config.lifecycle_interval();
    let sweep = config.sweep_interval();
    let wait = config.idempotency_wait();
    let cap = config.idempotency_wait_max();

    // Assert
    assert_eq!(
        window,
        config
            .heartbeat_secs
            .saturating_mul(config.missed_heartbeats)
    );
    assert_eq!(budget, Duration::from_secs(config.ping_budget_secs));
    assert_eq!(poll, Duration::from_millis(config.poll_interval_ms));
    assert_eq!(ping, Duration::from_millis(config.ping_interval_ms));
    assert_eq!(timeout, Duration::from_millis(config.ping_timeout_ms));
    assert_eq!(
        lifecycle,
        Duration::from_millis(config.lifecycle_interval_ms)
    );
    assert_eq!(sweep, Duration::from_secs(config.sweep_interval_secs));
    assert_eq!(config.sweep_batch_size(), config.sweep_batch as usize);
    assert_eq!(config.fetch_batch_size(), config.batch_size as usize);
    assert_eq!(wait, Duration::from_millis(config.idempotency_wait_ms));
    assert_eq!(cap, Duration::from_millis(config.idempotency_wait_max_ms));
}

#[test]
fn given_byte_suffixes_when_parsed_then_binary_units_apply_and_overflow_fails() {
    // Arrange
    let huge = format!("{}G", u64::MAX);

    // Act
    let kilobytes = parse_byte_size("2kb").unwrap();
    let megabytes = parse_byte_size("3MB").unwrap();
    let gigabytes = parse_byte_size("1gb").unwrap();
    let bytes = parse_byte_size("9b").unwrap();
    let empty = parse_byte_size("").unwrap_err();
    let blank = parse_byte_size("  ").unwrap_err();
    let suffix_only = parse_byte_size("G").unwrap_err();
    let overflow = parse_byte_size(&huge).unwrap_err();

    // Assert
    assert_eq!(kilobytes, 2 * 1024);
    assert_eq!(megabytes, 3 * 1024 * 1024);
    assert_eq!(gigabytes, 1024 * 1024 * 1024);
    assert_eq!(bytes, 9);
    assert!(empty.message.contains("greater than zero"));
    assert!(blank.message.contains("greater than zero"));
    assert!(suffix_only.message.contains('G'));
    assert!(overflow.message.contains("greater than zero"));
}

#[test]
fn given_child_memory_and_a_bad_ratio_when_from_env_then_the_range_is_enforced() {
    // Arrange
    let _guard = ENV_LOCK.lock().unwrap();

    // Act
    let soft_above_hard = {
        let _soft = EnvGuard::set("SIMPLE_Q_CHILD_MEMORY_SOFT", "2M");
        let _hard = EnvGuard::set("SIMPLE_Q_CHILD_MEMORY_HARD", "1M");
        Config::from_env().unwrap_err().to_string()
    };
    let invalid_bytes = {
        let _soft = EnvGuard::set("SIMPLE_Q_CHILD_MEMORY_SOFT", "abc");
        Config::from_env().unwrap_err().to_string()
    };
    let nan_ratio = {
        let _ratio = EnvGuard::set("SIMPLE_Q_MEMORY_SOFT_RATIO", "nan");
        Config::from_env().unwrap_err().to_string()
    };
    let equal_ratios = {
        let _soft = EnvGuard::set("SIMPLE_Q_MEMORY_SOFT_RATIO", "0.5");
        let _hard = EnvGuard::set("SIMPLE_Q_MEMORY_HARD_RATIO", "0.5");
        Config::from_env().unwrap_err().to_string()
    };
    let deliveries = {
        let _max = EnvGuard::set(
            "SIMPLE_Q_MAX_DELIVERIES",
            &(u64::from(u32::MAX) + 1).to_string(),
        );
        Config::from_env().unwrap_err().to_string()
    };
    let accepted = {
        let _soft = EnvGuard::set("SIMPLE_Q_CHILD_MEMORY_SOFT", "1M");
        let _hard = EnvGuard::set("SIMPLE_Q_CHILD_MEMORY_HARD", "2M");
        let _done = EnvGuard::set("SIMPLE_Q_DONE_TTL_SECS", "0");
        let _dead = EnvGuard::set("SIMPLE_Q_DEAD_TTL_SECS", "0");
        Config::from_env().unwrap()
    };
    let zero_ttl = Config::defaults().with_unique_ttl(0).unwrap_err();
    let updated = Config::defaults().with_unique_ttl(10).unwrap();

    // Assert
    let _error: &dyn std::error::Error = &zero_ttl;
    assert!(
        soft_above_hard.contains("SIMPLE_Q_CHILD_MEMORY_SOFT"),
        "{soft_above_hard}"
    );
    assert!(
        invalid_bytes.contains("SIMPLE_Q_CHILD_MEMORY_SOFT"),
        "{invalid_bytes}"
    );
    assert!(nan_ratio.contains("(0, 1)"), "{nan_ratio}");
    assert!(
        equal_ratios.contains("SIMPLE_Q_MEMORY_SOFT_RATIO"),
        "{equal_ratios}"
    );
    assert!(
        deliveries.contains("SIMPLE_Q_MAX_DELIVERIES"),
        "{deliveries}"
    );
    assert_eq!(accepted.child_memory_soft, Some(1024 * 1024));
    assert_eq!(accepted.child_memory_hard, Some(2 * 1024 * 1024));
    assert_eq!(accepted.done_ttl_secs, 0);
    assert_eq!(accepted.dead_ttl_secs, 0);
    assert!(zero_ttl.to_string().contains("greater than zero"));
    assert_eq!(updated.unique_ttl_secs, 10);
}
