use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;

use crate::bridge;
use crate::job::Job;
use crate::redis_api::{connection, queue_config};
use crate::registry;

#[derive(Clone)]
pub struct Shared {
    pub completed: Arc<AtomicU64>,
    pub draining: Arc<AtomicBool>,
    pub shutdown: Arc<AtomicBool>,
    pub pool: WorkerPool,
    pub max_tasks: u64,
}

#[derive(Clone, Default)]
pub struct WorkerPool {
    workers: Arc<Mutex<Vec<WorkerContext>>>,
}

impl WorkerPool {
    pub fn remember(&self, worker: &WorkerContext) {
        let mut workers = self.workers.lock().expect("worker pool");
        if workers.iter().any(|known| known.name() == worker.name()) {
            return;
        }
        workers.push(worker.clone());
    }

    pub fn pause_all(&self) {
        for worker in self.workers.lock().expect("worker pool").iter() {
            let _ = worker.pause();
        }
    }

    pub fn stop_all(&self) {
        for worker in self.workers.lock().expect("worker pool").iter() {
            let _ = worker.stop();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.workers.lock().expect("worker pool").is_empty()
    }
}

#[derive(Clone)]
struct QueueState {
    queue: String,
    shared: Shared,
}

pub async fn run(queues: Vec<String>, concurrency: usize, shared: Shared) -> Result<()> {
    let mut monitor = Monitor::new();

    for queue in queues {
        let backend = RedisStorage::<Job>::new(connection()?).with_config(queue_config(&queue)?);
        let state = QueueState {
            queue: queue.clone(),
            shared: shared.clone(),
        };
        let pool = shared.pool.clone();
        let worker_name = format!("agent-worker.{queue}.{}", std::process::id());

        monitor = monitor.register(move |_| {
            let pool = pool.clone();
            WorkerBuilder::new(worker_name.clone())
                .backend(backend.clone())
                .data(state.clone())
                .concurrency(concurrency)
                .on_event(move |worker, event| {
                    if matches!(event, Event::Start) {
                        pool.remember(worker);
                    }
                })
                .build(handle)
        });
    }

    monitor
        .run()
        .await
        .map_err(|error| anyhow::anyhow!("worker monitor failed: {error}"))?;
    Ok(())
}

async fn handle(
    job: Job,
    task: TaskContext,
    worker: WorkerContext,
    state: Data<QueueState>,
) -> Result<serde_json::Value, BoxDynError> {
    state.shared.pool.remember(&worker);
    if state.shared.draining.load(Ordering::SeqCst) || state.shared.shutdown.load(Ordering::SeqCst)
    {
        state.shared.pool.pause_all();
    }

    let task_id = task.task_id().to_owned();
    let result = bridge::execute(&state.queue, &job, &task_id).await;

    let finished = state.shared.completed.fetch_add(1, Ordering::AcqRel) + 1;
    if finished >= state.shared.max_tasks {
        state.shared.draining.store(true, Ordering::SeqCst);
        state.shared.pool.pause_all();
        tracing::info!(
            finished,
            max_tasks = state.shared.max_tasks,
            in_flight = registry::len(),
            "process reached max tasks; draining active handlers"
        );
    }

    match result {
        // apalis-redis 1.0.0 reschedules every Failed result while attempt < 25
        // and ignores TaskBuilder::max_attempts. A finished Python error is
        // dead-lettered as Killed so max_retries=0 is not turned into 25 retries.
        // A process crash still leaves the message inflight for orphan redelivery.
        Ok(value) => Ok(value),
        Err(error) => Err(Box::new(AbortError::new(error))),
    }
}
