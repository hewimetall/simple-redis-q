use super::*;

#[test]
fn a_minute_of_silence_does_not_kill_under_the_default_budget() {
    // Arrange

    let started = Instant::now();
    // Act

    let mut watch = PingWatch::new(crate::names::default_ping_budget());
    // Assert

    assert!(!watch.observe(false, started));
    assert!(!watch.observe(false, started + Duration::from_secs(60)));
}

#[test]
fn silence_past_a_short_budget_kills_and_recovery_resets_it() {
    // Arrange

    let started = Instant::now();
    // Act

    let mut watch = PingWatch::new(Duration::from_secs(30));
    // Assert

    assert!(!watch.observe(false, started));
    assert!(watch.observe(false, started + Duration::from_secs(30)));

    let mut watch = PingWatch::new(Duration::from_secs(30));
    assert!(!watch.observe(false, started));
    assert!(!watch.observe(true, started + Duration::from_secs(10)));
    assert!(!watch.observe(false, started + Duration::from_secs(40)));
}
