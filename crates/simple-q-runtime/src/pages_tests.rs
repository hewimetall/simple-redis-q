use super::*;

#[test]
fn the_dead_index_is_owned_by_this_repo_and_the_page_cap_is_200() {
    // Arrange
    // Act
    // Assert

    assert_eq!(dead_index_key("default"), "simple_q:dead:default");
    assert_eq!(PAGE_MAX, 200);
    assert!(bound_page_limit(0).is_err());
    assert_eq!(bound_page_limit(1).unwrap(), 1);
    assert_eq!(bound_page_limit(50).unwrap(), 50);
    assert_eq!(bound_page_limit(200).unwrap(), 200);
    assert_eq!(bound_page_limit(201).unwrap(), 200);
    assert_eq!(bound_page_limit(10_000).unwrap(), 200);
}

#[test]
fn cursors_round_trip_and_reject_a_different_kind() {
    // Arrange

    let dead = encode_score_cursor("d", "1700000000", "01HZTASK");
    let scheduled = encode_score_cursor("s", "1700000001.0", "01HZLATER");
    let active = encode_active_cursor(2);
    let task_id = encode_active_cursor_bytes(b"a\n01HZACTIVE");
    match decode_cursor(Some(&dead)).unwrap() {
        Some(Decoded::Score {
            kind,
            score,
            member,
        }) => {
            assert_eq!(kind, "d");
            assert_eq!(score, "1700000000");
            assert_eq!(member, "01HZTASK");
        }
        other => panic!("dead cursor decoded as {other:?}"),
    }
    // Act

    match decode_cursor(Some(&scheduled)).unwrap() {
        Some(Decoded::Score {
            kind,
            score,
            member,
        }) => {
            assert_eq!(kind, "s");
            assert_eq!(score, "1700000001");
            assert_eq!(member, "01HZLATER");
        }
        other => panic!("scheduled cursor decoded as {other:?}"),
    }
    // Assert

    assert!(matches!(
        decode_cursor(Some(&active)).unwrap(),
        Some(Decoded::Active(offset)) if offset == "2"
    ));
    assert_eq!(active_cursor(Some(&active)).unwrap(), 2);
    assert_eq!(active_cursor(None).unwrap(), 0);
    assert!(decode_cursor(Some(&task_id)).is_err());
    assert!(active_cursor(Some(&task_id)).is_err());
    assert!(decode_cursor(None).unwrap().is_none());
    assert!(decode_cursor(Some("")).unwrap().is_none());
    assert!(decode_cursor(Some("%%%")).is_err());
    assert!(dead_cursor(Some(&active)).is_err());
    assert!(scheduled_cursor(Some(&dead)).is_err());
    assert!(active_cursor(Some(&scheduled)).is_err());
    assert_eq!(dead_cursor(None).unwrap(), (String::new(), String::new()));
}

#[test]
fn stored_error_text_stops_at_4_kib() {
    // Arrange
    // Act
    // Assert

    assert_eq!(cap_error("RuntimeError: boom"), "RuntimeError: boom");
    let long = "x".repeat(8_000);
    let capped = cap_error(&long);
    assert_eq!(capped.len(), ERROR_MAX_BYTES);
    assert!(long.starts_with(capped));
    let mut split = "y".repeat(ERROR_MAX_BYTES - 1);
    split.push('я');
    let capped = cap_error(&split);
    assert!(capped.len() < ERROR_MAX_BYTES);
    assert!(split.starts_with(capped));
    assert!(capped.is_char_boundary(capped.len()));
}

fn encode_active_cursor_bytes(bytes: &[u8]) -> String {
    b64url_encode(bytes)
}
