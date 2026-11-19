use super::*;

#[test]
fn given_two_executions_when_one_is_removed_then_snapshot_and_lines_keep_the_other() {
    // Arrange
    let id = format!("exec-{}", std::process::id());
    let other = format!("{id}-b");
    let before = len();
    insert(id.clone(), "render_report".to_owned(), "default".to_owned());
    insert(other.clone(), "render_report".to_owned(), "reports".to_owned());

    // Act
    remove(&id);
    let left = snapshot();
    let text = lines();

    // Assert
    assert_eq!(len(), before + 1);
    assert!(left.iter().all(|item| item.id != id));
    let kept = left.iter().find(|item| item.id == other).expect("kept");
    assert_eq!(kept.task, "render_report");
    assert_eq!(kept.queue, "reports");
    assert_eq!(kept.stage, "handler");
    assert!(kept.updated_ms >= kept.started_ms);
    assert!(text.iter().any(|line| {
        line.starts_with(&other)
            && line.contains("task=render_report")
            && line.contains("queue=reports")
            && line.contains("stage=handler")
            && line.contains("updated_ms=")
    }));
    remove(&other);
    assert_eq!(len(), before);
    remove("missing");
    assert_eq!(len(), before);
}
