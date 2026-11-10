//! One delivery of one job.
//!
//! Poison is decided here, from the delivery count, before Python is called.
//! Retry and timeout come back from Python, because they need the exception.
//! Redis is the server side (`redis_api`). The handler sees a copy of the
//! message. Retry stores the original job, so the sender's headers stay in Redis.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::bridge;
use crate::telemetry;
use simple_q_runtime::ids::worker_name;
use simple_q_runtime::job::{Decision, Job, decide};
use simple_q_runtime::redis_api::{self, connection, queue_config};
use simple_q_runtime::registry;

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
    shared: Shared,
}

pub async fn run(queues: Vec<String>, concurrency: usize, shared: Shared) -> Result<()> {
    let mut monitor = Monitor::new();

    for queue in queues {
        let backend = RedisStorage::<Job>::new(connection()?).with_config(queue_config(&queue)?);
        let state = QueueState {
            shared: shared.clone(),
        };
        let pool = shared.pool.clone();
        let worker_name = worker_name(&queue);

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
    let span = tracing::info_span!(
        "process",
        otel.name = %format!("process {}", job.actor_name),
        otel.kind = "consumer",
        messaging.system = "redis",
        messaging.destination = %job.queue,
        messaging.operation = "process",
        messaging.message.id = %job.message_id,
        actor_name = %job.actor_name,
    );
    if let Some(parent) = telemetry::parent_context(&job.headers) {
        let _ = span.set_parent(parent);
    }
    if let Some(link) = telemetry::link_context(&job.headers) {
        span.add_link(link);
    }
    let dispatched = telemetry::with_span_traceparent(&job, &span);
    handle_job(job, dispatched, task, worker, state)
        .instrument(span)
        .await
}

async fn handle_job(
    job: Job,
    dispatched: Job,
    task: TaskContext,
    worker: WorkerContext,
    state: Data<QueueState>,
) -> Result<serde_json::Value, BoxDynError> {
    if let Err(error) = job.validate() {
        tracing::warn!(
            message_id = %job.message_id,
            actor_name = %job.actor_name,
            queue_name = %job.queue,
            error = %error,
            "rejected an invalid message"
        );
        if let Err(hook_error) = bridge::note_dead(&job, "invalid").await {
            tracing::warn!(
                message_id = %job.message_id,
                actor_name = %job.actor_name,
                error = %hook_error,
                "dead-letter hook failed for an invalid message"
            );
        }
        return Err(Box::new(AbortError::new(error)));
    }

    state.shared.pool.remember(&worker);
    if state.shared.draining.load(Ordering::SeqCst) || state.shared.shutdown.load(Ordering::SeqCst)
    {
        state.shared.pool.pause_all();
    }

    let task_id = task.task_id().to_owned();
    let deliveries = match redis_api::incr_delivery(&job.message_id, job.retries).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(
                message_id = %job.message_id,
                actor_name = %job.actor_name,
                queue_name = %job.queue,
                retries = job.retries,
                error = %error,
                "delivery count failed"
            );
            std::process::exit(2);
        }
    };
    if decide(deliveries, job.max_deliveries) == Decision::Poison {
        tracing::warn!(
            message_id = %job.message_id,
            actor_name = %job.actor_name,
            queue_name = %job.queue,
            retries = job.retries,
            deliveries,
            max_deliveries = job.max_deliveries,
            "burying a poison message before the handler"
        );
        if let Err(hook_error) = bridge::note_dead(&job, "poison").await {
            tracing::warn!(
                message_id = %job.message_id,
                actor_name = %job.actor_name,
                error = %hook_error,
                "dead-letter hook failed for a poison message"
            );
        }
        bridge::note_reject(&job.queue, &job.actor_name);
        note_finished(&state);
        return Err(Box::new(AbortError::new(anyhow::anyhow!("dead: poison"))));
    }
    tracing::info!(
        message_id = %job.message_id,
        actor_name = %job.actor_name,
        queue_name = %job.queue,
        retries = job.retries,
        deliveries,
        "running actor"
    );
    let result = bridge::execute(&dispatched, &task_id, deliveries).await;

    note_finished(&state);

    let value = match result {
        Ok(value) => value,
        Err(error) => return Err(Box::new(AbortError::new(error))),
    };
    match value.get("outcome").and_then(|item| item.as_str()) {
        Some("retry") => {
            let delay_ms = value
                .get("delay_ms")
                .and_then(|item| item.as_u64())
                .unwrap_or(0);
            let mut next = job.clone();
            next.retries = next.retries.saturating_add(1);
            if let Err(error) =
                redis_api::enqueue_job(next, Some(std::time::Duration::from_millis(delay_ms))).await
            {
                tracing::error!(
                    message_id = %job.message_id,
                    actor_name = %job.actor_name,
                    queue_name = %job.queue,
                    retries = job.retries,
                    error = %error,
                    "retry enqueue failed; leaving the attempt inflight"
                );
                std::process::exit(2);
            }
            bridge::note_retry(&job.queue, &job.actor_name);
            Ok(value)
        }
        Some("dead") => Err(Box::new(AbortError::new(anyhow::anyhow!(
            "dead: {}",
            value
                .get("reason")
                .and_then(|item| item.as_str())
                .unwrap_or("dead")
        )))),
        _ => Ok(value),
    }
}

fn note_finished(state: &Data<QueueState>) {
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
}
