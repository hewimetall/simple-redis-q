use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Execution {
    pub id: String,
    pub task: String,
    pub queue: String,
    pub stage: &'static str,
    pub started_ms: u64,
    pub updated_ms: u64,
}

static EXECUTIONS: Mutex<BTreeMap<String, Execution>> = Mutex::new(BTreeMap::new());

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub fn insert(id: String, task: String, queue: String) {
    let now = now_ms();
    let mut executions = EXECUTIONS.lock().expect("execution registry");
    executions.insert(
        id.clone(),
        Execution {
            id,
            task,
            queue,
            stage: "handler",
            started_ms: now,
            updated_ms: now,
        },
    );
}

pub fn remove(id: &str) {
    EXECUTIONS.lock().expect("execution registry").remove(id);
}

pub fn len() -> usize {
    EXECUTIONS.lock().expect("execution registry").len()
}

/// Last known executions. This does not read Python objects.
pub fn snapshot() -> Vec<Execution> {
    EXECUTIONS
        .lock()
        .expect("execution registry")
        .values()
        .cloned()
        .collect()
}

pub fn lines() -> Vec<String> {
    snapshot()
        .into_iter()
        .map(|execution| {
            format!(
                "{} task={} queue={} stage={} updated_ms={}",
                execution.id,
                execution.task,
                execution.queue,
                execution.stage,
                execution.updated_ms
            )
        })
        .collect()
}
