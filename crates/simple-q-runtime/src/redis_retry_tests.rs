use std::io::ErrorKind;
use std::time::Duration;

use redis::ErrorKind as RedisKind;
use redis::RetryMethod;

use super::*;

fn broken_pipe() -> RedisError {
    RedisError::from(std::io::Error::new(ErrorKind::BrokenPipe, "broken pipe"))
}

fn timed_out() -> RedisError {
    RedisError::from(std::io::Error::new(ErrorKind::TimedOut, "timed out"))
}

#[test]
fn a_dropped_heartbeat_is_replayed_and_a_fetch_is_not() {
    // Arrange

    let pipe = broken_pipe();
    // Act

    // Assert

    assert!(replay(b"*1\r\n$4\r\nPING\r\n", &pipe));
    assert!(!replay(FETCH_SHA, &pipe));
    assert!(!replay(
        b"*2\r\n$7\r\nEVALSHA\r\n$40\r\nF053FD9B41B61A2D32B8B7D8DBA3F1A96E3AA6E1\r\n",
        &pipe
    ));
    assert!(!replay(ENQUEUE_SHA, &pipe));
    let eval = b"*2\r\n$4\r\nEVAL\r\n$15\r\nCant fetch next\r\n";
    assert!(!replay(eval, &pipe));
    let push = b"*2\r\n$4\r\nEVAL\r\n$19\r\nthe idempotency_ttl\r\n";
    assert!(!replay(push, &pipe));
}

#[test]
fn a_timeout_is_replayed_and_a_script_error_is_not() {
    // Arrange
    // Act
    // Assert

    assert!(replay(b"*1\r\n$4\r\nPING\r\n", &timed_out()));
    let script = RedisError::from((RedisKind::Client, "task not executed by worker"));
    assert!(!replay(b"*1\r\n$4\r\nPING\r\n", &script));
}

#[test]
fn the_same_words_inside_an_ack_payload_do_not_block_a_replay() {
    // Arrange

    let pipe = broken_pipe();
    // Act

    let ack = b"*3\r\n$7\r\nEVALSHA\r\n$40\r\n9a702ec86f2a2dcdd3f53342b4e7cdce332871cd\r\n$15\r\nCant fetch next\r\n";
    // Assert

    assert!(replay(ack, &pipe));
}

#[test]
fn given_a_script_load_of_fetch_text_when_the_pipe_breaks_then_it_is_not_replayed() {
    // Arrange
    let pipe = broken_pipe();
    let fetch = b"*2\r\n$6\r\nSCRIPT\r\n$15\r\nCant fetch next\r\n";
    let ack = b"*2\r\n$6\r\nSCRIPT\r\n$3\r\nACK\r\n";

    // Act
    let fetch_replay = replay(fetch, &pipe);
    let ack_replay = replay(ack, &pipe);
    let empty = claims_on_success(b"");
    let short = contains_ignore_ascii_case(b"ab", b"abc");

    // Assert
    assert!(!fetch_replay);
    assert!(ack_replay);
    assert!(!empty);
    assert!(!short);
}

#[test]
fn given_repeated_attempts_when_pause_is_chosen_then_it_grows_and_caps_at_one_second() {
    // Arrange
    let wait = RetryMethod::WaitAndRetry;

    // Act
    let first = pause_for(RetryMethod::RetryImmediately, 0);
    let later = pause_for(RetryMethod::RetryImmediately, 10);
    let waited = pause_for(wait, 0);
    let reconnect = pause_for(RetryMethod::Reconnect, 6);

    // Assert
    assert_eq!(first, Duration::from_millis(20));
    assert_eq!(later, Duration::from_millis(1_000));
    assert_eq!(waited, Duration::from_millis(50));
    assert_eq!(reconnect, Duration::from_millis(1_000));
}
