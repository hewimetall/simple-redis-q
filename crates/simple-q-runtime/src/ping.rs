//! Business rule: how long a silent interpreter may stay silent.
//!
//! No Redis and no Python. The child supplies the ping result and the clock.

use std::time::{Duration, Instant};

/// Kill the child only after the loop has stayed silent for the whole budget.
///
/// The default budget is the default time limit plus one minute, so a blocking
/// section of one minute does not look like a dead interpreter.
#[derive(Debug, Clone)]
pub struct PingWatch {
    budget: Duration,
    failing_since: Option<Instant>,
}

impl PingWatch {
    pub fn new(budget: Duration) -> Self {
        Self {
            budget,
            failing_since: None,
        }
    }

    /// Return whether the child should exit. `ok` is one ping result at `now`.
    pub fn observe(&mut self, ok: bool, now: Instant) -> bool {
        if ok {
            self.failing_since = None;
            return false;
        }
        let started = *self.failing_since.get_or_insert(now);
        now.saturating_duration_since(started) >= self.budget
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minute_of_silence_does_not_kill_under_the_default_budget() {
        let started = Instant::now();
        let mut watch = PingWatch::new(crate::names::default_ping_budget());
        assert!(!watch.observe(false, started));
        assert!(!watch.observe(false, started + Duration::from_secs(60)));
    }

    #[test]
    fn silence_past_a_short_budget_kills_and_recovery_resets_it() {
        let started = Instant::now();
        let mut watch = PingWatch::new(Duration::from_secs(30));
        assert!(!watch.observe(false, started));
        assert!(watch.observe(false, started + Duration::from_secs(30)));

        let mut watch = PingWatch::new(Duration::from_secs(30));
        assert!(!watch.observe(false, started));
        assert!(!watch.observe(true, started + Duration::from_secs(10)));
        assert!(!watch.observe(false, started + Duration::from_secs(40)));
    }
}
