use std::os::unix::ffi::OsStrExt;

use super::*;

#[test]
fn a_thousand_slot_recycles_do_not_grow_the_directory() {
    // Arrange

    let dir = std::env::temp_dir().join(format!("simple-q-slot-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("counter_3.db"), b"keep").unwrap();

    for _ in 0..1000 {
        fs::write(dir.join("gauge_livesum_3.db"), b"stale").unwrap();
        fs::write(dir.join("gauge_liveall_3.db"), b"stale").unwrap();
        fs::write(dir.join("gauge_livesum_99.db"), b"other").unwrap();
        reset_slot_gauges(&dir, 3).unwrap();
        let _ = fs::remove_file(dir.join("gauge_livesum_99.db"));
    }
    // Act

    let names = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    // Assert

    assert_eq!(names, vec!["counter_3.db".to_owned()]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn given_a_missing_directory_when_gauges_are_reset_then_nothing_is_removed() {
    // Arrange
    let dir = std::env::temp_dir().join(format!(
        "simple-q-missing-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&dir);

    // Act
    let removed = reset_slot_gauges(&dir, 1).unwrap();

    // Assert
    assert_eq!(removed, 0);
    assert!(!dir.exists());
}

#[test]
fn given_db_files_and_a_non_utf8_name_when_wiped_then_only_utf8_db_files_go() {
    // Arrange
    let dir = std::env::temp_dir().join(format!(
        "simple-q-wipe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&dir);
    assert_eq!(wipe_db_files(&dir).unwrap(), 0);
    assert!(dir.is_dir());
    fs::write(dir.join("keep.txt"), b"keep").unwrap();
    fs::write(dir.join("counter_1.db"), b"db").unwrap();
    fs::write(dir.join("gauge_livesum_1.db"), b"stale").unwrap();
    let odd = dir.join(std::ffi::OsStr::from_bytes(b"gauge_live_\xff_1.db"));
    fs::write(&odd, b"odd").unwrap();

    // Act
    let reset = reset_slot_gauges(&dir, 1).unwrap();
    let wiped = wipe_db_files(&dir).unwrap();

    // Assert
    assert_eq!(reset, 1);
    assert!(!dir.join("gauge_livesum_1.db").exists());
    assert_eq!(wiped, 1);
    assert!(!dir.join("counter_1.db").exists());
    assert!(dir.join("keep.txt").is_file());
    assert!(odd.is_file());
    let _ = fs::remove_dir_all(&dir);
}
