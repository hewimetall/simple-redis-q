use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Wire payload stored by the Redis backend.
///
/// Dependency-injected Python objects are not part of the message.
/// The worker-side controller reconstructs them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub task: String,
    pub payload: Value,
}
