use super::{page_error_is_value, page_error_text, resolve_lock_ttl};

#[test]
fn lock_ttl_accepts_either_name() {
    // Arrange
    // Act
    // Assert

    assert_eq!(resolve_lock_ttl(Some(30), None).unwrap(), 30);
    assert_eq!(resolve_lock_ttl(None, Some(30)).unwrap(), 30);
    assert_eq!(resolve_lock_ttl(Some(30), Some(30)).unwrap(), 30);
    assert!(resolve_lock_ttl(None, None).is_err());
    assert!(resolve_lock_ttl(Some(1), Some(2)).is_err());
}

#[test]
fn a_wrapped_cursor_error_is_a_value_error() {
    // Arrange

    let error = anyhow::anyhow!("cursor is not in the active list")
        .context("failed to page the active list");
    // Act

    // Assert

    assert!(!page_error_is_value(&error.to_string()));
    let text = page_error_text(&error);
    assert!(page_error_is_value(&text), "{text}");
    assert!(text.contains("cursor is not in the active list"));
}
