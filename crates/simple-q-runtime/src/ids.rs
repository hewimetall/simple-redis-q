use std::process;

use uuid::Uuid;

pub fn lock_owner() -> String {
    Uuid::new_v4().to_string()
}

pub fn worker_name(queue: &str) -> String {
    format!(
        "agent-worker.{queue}.{}.{}.{}",
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
mod tests {
    use super::*;

    #[test]
    fn lock_owners_are_distinct_uuids_not_pids() {
        let first = lock_owner();
        let second = lock_owner();
        assert_ne!(first, second);
        assert!(Uuid::parse_str(&first).is_ok());
        assert_ne!(first, process::id().to_string());
    }

    #[test]
    fn worker_names_include_host_pid_and_a_random_suffix() {
        let queue = "default";
        let first = worker_name(queue);
        let second = worker_name(queue);
        assert_ne!(first, second);
        assert!(first.starts_with(&format!(
            "agent-worker.{queue}.{}.{}",
            hostname(),
            process::id()
        )));
    }
}
