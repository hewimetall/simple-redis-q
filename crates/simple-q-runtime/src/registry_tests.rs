use super::*;

#[test]
fn given_two_executions_when_one_is_removed_then_snapshot_and_lines_keep_the_other() {
    // Arrange
    let id = format!("exec-{}", std::process::id());
    let other = format!("{id}-b");
    let before = len();
    insert(id.clone(), "recalc".to_owned(), "default".to_owned());
    insert(other.clone(), "recalc".to_owned(), "quota".to_owned());

    // Act
    remove(&id);
    let left = snapshot();
    let text = lines();

    // Assert
    assert_eq!(len(), before + 1);
    assert!(left.iter().all(|item| item.id != id));
    let kept = left.iter().find(|item| item.id == other).expect("kept");
    assert_eq!(kept.task, "recalc");
    assert_eq!(kept.queue, "quota");
    assert_eq!(kept.stage, "handler");
    assert!(kept.updated_ms >= kept.started_ms);
    assert!(text.iter().any(|line| {
        line.starts_with(&other)
            && line.contains("task=recalc")
            && line.contains("queue=quota")
            && line.contains("stage=handler")
            && line.contains("updated_ms=")
    }));
    remove(&other);
    assert_eq!(len(), before);
    remove("missing");
    assert_eq!(len(), before);
}
