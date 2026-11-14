//! Spike: can one Oxana Worker process an opaque JSON envelope (our Job shape)?
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
enum SpikeError {
    #[error("bridge")]
    Bridge,
}

#[derive(Debug, Clone)]
struct Ctx;

/// Opaque payload: one Rust job type for all Python actors.
#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct OpaqueJob {
    message: serde_json::Value,
}

#[derive(oxana::Worker)]
#[oxana(context = Ctx, error = SpikeError)]
struct OpaqueWorker;

impl OpaqueWorker {
    async fn process(&self, job: OpaqueJob, _ctx: &oxana::JobContext) -> Result<(), SpikeError> {
        let _ = job.message.get("actor_name");
        Ok(())
    }
}

#[derive(oxana::Queue)]
#[oxana(key = "spike_opaque", concurrency = 1)]
struct SpikeQueue;

fn main() {
    // Compile-time proof only: types implement Job/Worker/Queue.
    let _ = std::any::type_name::<OpaqueJob>();
    let _ = std::any::type_name::<OpaqueWorker>();
    let _ = std::any::type_name::<SpikeQueue>();
    println!("spike-oxana-opaque: compile ok oxana=2.5.0 OpaqueJob={{message: Value}}");
}
