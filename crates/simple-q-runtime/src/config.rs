//! Every `SIMPLE_Q_*` setting. `Config::from_env` is the only reader of those variables.

use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::names::{
    DEFAULT_BATCH_SIZE, DEFAULT_DEAD_TTL_SECS, DEFAULT_DONE_TTL_SECS,
    DEFAULT_IDEMPOTENCY_WAIT_MAX_MS, DEFAULT_IDEMPOTENCY_WAIT_MS, DEFAULT_LIFECYCLE_INTERVAL_MS,
    DEFAULT_MAX_DELIVERIES, DEFAULT_PING_INTERVAL_MS, DEFAULT_PING_TIMEOUT_MS,
    DEFAULT_POLL_INTERVAL_MS, DEFAULT_SWEEP_INTERVAL_SECS, DEFAULT_TIME_LIMIT_MS,
    DEFAULT_UNIQUE_TTL_SECS, HEARTBEAT_INTERVAL_SECS, MISSED_HEARTBEATS, PING_MARGIN_SECS,
    SWEEP_BATCH,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigError {
    message: String,
}

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Process settings. Absent variables keep the built-in defaults.
/// A value that does not parse, is out of range, or is zero where zero is illegal
/// fails construction.
#[pyclass]
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    #[pyo3(get)]
    pub heartbeat_secs: u64,
    #[pyo3(get)]
    pub missed_heartbeats: u64,
    /// Milliseconds between idle fetches. Independent of `heartbeat_secs`.
    #[pyo3(get)]
    pub poll_interval_ms: u64,
    /// Milliseconds between Python pings. The lifecycle tick is the wake-up.
    #[pyo3(get)]
    pub ping_interval_ms: u64,
    /// Milliseconds for one stage of `ping`: schedule the callback, then wait for it.
    #[pyo3(get)]
    pub ping_timeout_ms: u64,
    /// Milliseconds between lifecycle wakes. Drain and a due ping are noticed here.
    #[pyo3(get)]
    pub lifecycle_interval_ms: u64,
    /// Seconds between sweeps of done and dead records.
    #[pyo3(get)]
    pub sweep_interval_secs: u64,
    /// Records removed in one sweep cursor step.
    #[pyo3(get)]
    pub sweep_batch: u64,
    /// Tasks fetched in one Redis read. Ack and retry stay on `lock_tasks` and re-enqueue.
    #[pyo3(get)]
    pub batch_size: u64,
    /// First pause before the next idempotency terminal check.
    #[pyo3(get)]
    pub idempotency_wait_ms: u64,
    /// Cap of that pause. The pause doubles until it reaches this.
    #[pyo3(get)]
    pub idempotency_wait_max_ms: u64,
    #[pyo3(get)]
    pub ping_budget_secs: u64,
    #[pyo3(get)]
    pub done_ttl_secs: u64,
    #[pyo3(get)]
    pub dead_ttl_secs: u64,
    #[pyo3(get)]
    pub unique_ttl_secs: u64,
    #[pyo3(get)]
    pub max_deliveries: u32,
    #[pyo3(get)]
    pub memory_soft_ratio: f64,
    #[pyo3(get)]
    pub memory_hard_ratio: f64,
    #[pyo3(get)]
    pub child_memory_soft: Option<u64>,
    #[pyo3(get)]
    pub child_memory_hard: Option<u64>,
}

impl Config {
    pub fn defaults() -> Self {
        Self {
            heartbeat_secs: HEARTBEAT_INTERVAL_SECS,
            missed_heartbeats: MISSED_HEARTBEATS as u64,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            ping_interval_ms: DEFAULT_PING_INTERVAL_MS,
            ping_timeout_ms: DEFAULT_PING_TIMEOUT_MS,
            lifecycle_interval_ms: DEFAULT_LIFECYCLE_INTERVAL_MS,
            sweep_interval_secs: DEFAULT_SWEEP_INTERVAL_SECS,
            sweep_batch: SWEEP_BATCH as u64,
            batch_size: DEFAULT_BATCH_SIZE,
            idempotency_wait_ms: DEFAULT_IDEMPOTENCY_WAIT_MS,
            idempotency_wait_max_ms: DEFAULT_IDEMPOTENCY_WAIT_MAX_MS,
            ping_budget_secs: DEFAULT_TIME_LIMIT_MS / 1000 + PING_MARGIN_SECS,
            done_ttl_secs: DEFAULT_DONE_TTL_SECS,
            dead_ttl_secs: DEFAULT_DEAD_TTL_SECS,
            unique_ttl_secs: DEFAULT_UNIQUE_TTL_SECS,
            max_deliveries: DEFAULT_MAX_DELIVERIES,
            memory_soft_ratio: 0.80,
            memory_hard_ratio: 0.95,
            child_memory_soft: None,
            child_memory_hard: None,
        }
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let defaults = Self::defaults();
        let heartbeat_secs = read_u64("SIMPLE_Q_HEARTBEAT_SECS", defaults.heartbeat_secs, true)?;
        let missed_heartbeats = read_u64(
            "SIMPLE_Q_MISSED_HEARTBEATS",
            defaults.missed_heartbeats,
            true,
        )?;
        let poll_interval_ms =
            read_u64("SIMPLE_Q_POLL_INTERVAL_MS", defaults.poll_interval_ms, true)?;
        let ping_interval_ms =
            read_u64("SIMPLE_Q_PING_INTERVAL_MS", defaults.ping_interval_ms, true)?;
        let ping_timeout_ms = read_u64("SIMPLE_Q_PING_TIMEOUT_MS", defaults.ping_timeout_ms, true)?;
        let lifecycle_interval_ms = read_u64(
            "SIMPLE_Q_LIFECYCLE_INTERVAL_MS",
            defaults.lifecycle_interval_ms,
            true,
        )?;
        let sweep_interval_secs = read_u64(
            "SIMPLE_Q_SWEEP_INTERVAL_SECS",
            defaults.sweep_interval_secs,
            true,
        )?;
        let sweep_batch = read_u64("SIMPLE_Q_SWEEP_BATCH", defaults.sweep_batch, true)?;
        let batch_size = read_u64("SIMPLE_Q_BATCH_SIZE", defaults.batch_size, true)?;
        let idempotency_wait_ms = read_u64(
            "SIMPLE_Q_IDEMPOTENCY_WAIT_MS",
            defaults.idempotency_wait_ms,
            true,
        )?;
        let idempotency_wait_max_ms = read_u64(
            "SIMPLE_Q_IDEMPOTENCY_WAIT_MAX_MS",
            defaults.idempotency_wait_max_ms,
            true,
        )?;
        if idempotency_wait_max_ms < idempotency_wait_ms {
            return Err(ConfigError::new(
                "SIMPLE_Q_IDEMPOTENCY_WAIT_MAX_MS must be at least SIMPLE_Q_IDEMPOTENCY_WAIT_MS",
            ));
        }
        if usize::try_from(sweep_batch).is_err() || usize::try_from(batch_size).is_err() {
            return Err(ConfigError::new(
                "SIMPLE_Q_SWEEP_BATCH or SIMPLE_Q_BATCH_SIZE is out of range",
            ));
        }
        let ping_budget_secs =
            read_u64("SIMPLE_Q_PING_BUDGET_SECS", defaults.ping_budget_secs, true)?;
        let done_ttl_secs = read_u64("SIMPLE_Q_DONE_TTL_SECS", defaults.done_ttl_secs, false)?;
        let dead_ttl_secs = read_u64("SIMPLE_Q_DEAD_TTL_SECS", defaults.dead_ttl_secs, false)?;
        let unique_ttl_secs = read_u64("SIMPLE_Q_UNIQUE_TTL_SECS", defaults.unique_ttl_secs, true)?;
        let max_deliveries = read_u64(
            "SIMPLE_Q_MAX_DELIVERIES",
            u64::from(defaults.max_deliveries),
            true,
        )?;
        let max_deliveries = u32::try_from(max_deliveries)
            .map_err(|_| ConfigError::new("SIMPLE_Q_MAX_DELIVERIES is out of range"))?;
        let memory_soft_ratio =
            read_ratio("SIMPLE_Q_MEMORY_SOFT_RATIO", defaults.memory_soft_ratio)?;
        let memory_hard_ratio =
            read_ratio("SIMPLE_Q_MEMORY_HARD_RATIO", defaults.memory_hard_ratio)?;
        if memory_soft_ratio >= memory_hard_ratio {
            return Err(ConfigError::new(
                "SIMPLE_Q_MEMORY_SOFT_RATIO must be below SIMPLE_Q_MEMORY_HARD_RATIO",
            ));
        }
        let child_memory_soft = read_bytes("SIMPLE_Q_CHILD_MEMORY_SOFT")?;
        let child_memory_hard = read_bytes("SIMPLE_Q_CHILD_MEMORY_HARD")?;
        if let (Some(soft), Some(hard)) = (child_memory_soft, child_memory_hard) {
            if soft >= hard {
                return Err(ConfigError::new(
                    "SIMPLE_Q_CHILD_MEMORY_SOFT must be below SIMPLE_Q_CHILD_MEMORY_HARD",
                ));
            }
        }
        Ok(Self {
            heartbeat_secs,
            missed_heartbeats,
            poll_interval_ms,
            ping_interval_ms,
            ping_timeout_ms,
            lifecycle_interval_ms,
            sweep_interval_secs,
            sweep_batch,
            batch_size,
            idempotency_wait_ms,
            idempotency_wait_max_ms,
            ping_budget_secs,
            done_ttl_secs,
            dead_ttl_secs,
            unique_ttl_secs,
            max_deliveries,
            memory_soft_ratio,
            memory_hard_ratio,
            child_memory_soft,
            child_memory_hard,
        })
    }

    pub fn with_unique_ttl(mut self, secs: u64) -> Result<Self, ConfigError> {
        if secs == 0 {
            return Err(ConfigError::new(
                "SIMPLE_Q_UNIQUE_TTL_SECS must be greater than zero",
            ));
        }
        self.unique_ttl_secs = secs;
        Ok(self)
    }

    pub fn orphan_window_secs(&self) -> u64 {
        self.heartbeat_secs.saturating_mul(self.missed_heartbeats)
    }

    pub fn ping_budget(&self) -> Duration {
        Duration::from_secs(self.ping_budget_secs)
    }

    /// Idle fetch period. The worker heartbeat stays on `heartbeat_secs`.
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub fn ping_interval(&self) -> Duration {
        Duration::from_millis(self.ping_interval_ms)
    }

    /// One stage of `ping`. Scheduling and the callback wait each use this.
    pub fn ping_timeout(&self) -> Duration {
        Duration::from_millis(self.ping_timeout_ms)
    }

    pub fn lifecycle_interval(&self) -> Duration {
        Duration::from_millis(self.lifecycle_interval_ms)
    }

    pub fn sweep_interval(&self) -> Duration {
        Duration::from_secs(self.sweep_interval_secs)
    }

    pub fn sweep_batch_size(&self) -> usize {
        self.sweep_batch as usize
    }

    pub fn fetch_batch_size(&self) -> usize {
        self.batch_size as usize
    }

    pub fn idempotency_wait(&self) -> Duration {
        Duration::from_millis(self.idempotency_wait_ms)
    }

    pub fn idempotency_wait_max(&self) -> Duration {
        Duration::from_millis(self.idempotency_wait_max_ms)
    }
}

fn read_u64(name: &str, default: u64, positive: bool) -> Result<u64, ConfigError> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let value: u64 = raw
        .trim()
        .parse()
        .map_err(|_| ConfigError::new(format!("{name}={raw} does not parse as an integer")))?;
    if positive && value == 0 {
        return Err(ConfigError::new(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(value)
}

fn read_ratio(name: &str, default: f64) -> Result<f64, ConfigError> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let value: f64 = raw
        .trim()
        .parse()
        .map_err(|_| ConfigError::new(format!("{name}={raw} does not parse as a number")))?;
    if !value.is_finite() || value <= 0.0 || value >= 1.0 {
        return Err(ConfigError::new(format!("{name} must be in (0, 1)")));
    }
    Ok(value)
}

fn read_bytes(name: &str) -> Result<Option<u64>, ConfigError> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(None);
    };
    parse_byte_size(raw.trim())
        .map(Some)
        .map_err(|error| ConfigError::new(format!("{name} {}", error.message)))
}

#[derive(Debug)]
pub struct ByteSizeError {
    message: String,
}

impl ByteSizeError {
    fn invalid(raw: &str) -> Self {
        Self {
            message: format!("'{raw}' is invalid; the value must be greater than zero"),
        }
    }
}

/// `K`, `M`, and `G` are powers of 1024, with an optional `B`. A bare number is bytes.
pub fn parse_byte_size(raw: &str) -> Result<u64, ByteSizeError> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(ByteSizeError::invalid(raw));
    }
    let split_at = text
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    let (digits, suffix) = text.split_at(split_at);
    if digits.is_empty() {
        return Err(ByteSizeError::invalid(raw));
    }
    let suffix = suffix.trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "b" => 1_u64,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return Err(ByteSizeError::invalid(raw)),
    };
    let value: u64 = match digits.parse() {
        Ok(value) => value,
        Err(_) => return Err(ByteSizeError::invalid(raw)),
    };
    if value == 0 {
        return Err(ByteSizeError {
            message: format!("'{raw}' is invalid; the value must be greater than zero"),
        });
    }
    value
        .checked_mul(multiplier)
        .ok_or_else(|| ByteSizeError::invalid(raw))
}

#[pymethods]
impl Config {
    #[new]
    fn new() -> Self {
        Self::defaults()
    }

    #[classmethod]
    #[pyo3(name = "from_env")]
    fn from_env_py(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        Self::from_env().map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
        let config = Config::from_env().unwrap();
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
        }
        {
            let _ratio = EnvGuard::set("SIMPLE_Q_MEMORY_SOFT_RATIO", "1.5");
            let error = Config::from_env().unwrap_err().to_string();
            assert!(error.contains("SIMPLE_Q_MEMORY_SOFT_RATIO"), "{error}");
            assert!(error.contains("(0, 1)"), "{error}");
        }
    }

    #[test]
    fn zero_is_rejected_where_a_positive_value_is_required() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _ttl = EnvGuard::set("SIMPLE_Q_UNIQUE_TTL_SECS", "0");
        let error = Config::from_env().unwrap_err().to_string();
        assert!(error.contains("greater than zero"), "{error}");
    }
}
