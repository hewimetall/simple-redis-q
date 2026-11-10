//! Server side. Metric files for one worker slot, on the local disk.

use std::fs;
use std::path::Path;

/// Delete live-mode gauge files for one worker slot.
///
/// `prometheus_client` names them `gauge_livesum_<slot>.db`. `mark_process_dead`
/// removes the `gauge_live*` files and leaves counters, so a recycle reuses the
/// same slot files instead of creating a new pid file.
pub fn reset_slot_gauges(dir: &Path, slot: u32) -> std::io::Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    let suffix = format!("_{slot}.db");
    let mut removed = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with("gauge_live") && name.ends_with(&suffix) {
            fs::remove_file(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

pub fn wipe_db_files(dir: &Path) -> std::io::Result<usize> {
    if !dir.exists() {
        fs::create_dir_all(dir)?;
        return Ok(0);
    }
    let mut removed = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(".db") {
            fs::remove_file(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thousand_slot_recycles_do_not_grow_the_directory() {
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

        let names = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["counter_3.db".to_owned()]);
        let _ = fs::remove_dir_all(&dir);
    }
}
