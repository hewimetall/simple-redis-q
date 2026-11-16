//! Identifiers for locks and worker names.
//!
//! `hostname` is the only I/O here. It reads `HOSTNAME` or `/etc/hostname`.
//! Callers outside this module use `worker_name` and `lock_owner`.

use std::process;

use uuid::Uuid;

pub fn lock_owner() -> String {
    Uuid::new_v4().to_string()
}

pub fn worker_name(queue: &str) -> String {
    format!(
        "{}.{queue}.{}.{}.{}",
        crate::names::WORKER_NAME_PREFIX,
        hostname(),
        process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    )
}

pub fn hostname() -> String {
    if let Ok(value) = std::env::var("HOSTNAME") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return sanitize(trimmed);
        }
    }
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|value| sanitize(value.trim()))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "localhost".to_owned())
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|char| match char {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => char,
            _ => '-',
        })
        .collect()
}

#[cfg(test)]
#[path = "ids_tests.rs"]
mod tests;
