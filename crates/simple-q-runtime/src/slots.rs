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
#[path = "slots_tests.rs"]
mod tests;
