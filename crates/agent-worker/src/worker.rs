//! One delivery of one job.
//!
//! Poison is decided here, from the delivery count, before Python is called.
//! Retry and timeout come back from Python, because they need the exception.
//! Redis is the server side (`redis_api`). The handler sees a copy of the
//! message, including the delivery count and this process's worker name.
//! Retry stores the original job, so the sender's headers stay in Redis.
//! The delivery key is deleted only after ack. The idempotency key is
//! deleted only on a terminal outcome: ack, dead, or poison. A retry leaves
//! it in place, and a crash before that outcome leaves it for the redelivery.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;
use simple_q_runtime::redis_api::ConnectionManager;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::bridge;
use crate::telemetry;
use simple_q_runtime::cleanup::delete_worker_metadata;
use simple_q_runtime::config::Config;
use simple_q_runtime::ids::worker_name;
use simple_q_runtime::job::{Decision, Job, decide};
use simple_q_runtime::redis_api::{self, queue_config};
use simple_q_runtime::registry;

#[derive(Clone)]
pub struct Shared {
    pub completed: Arc<AtomicU64>,
    pub draining: Arc<AtomicBool>,
    pub shutdown: Arc<AtomicBool>,
    pub pool: WorkerPool,
    pub max_tasks: u64,
    /// `(queue, worker_name)` for each name passed to `WorkerBuilder::new`.
    pub worker_names: Arc<Mutex<Vec<(String, String)>>>,
    pub redis: ConnectionManager,
    pub config: Config,
    /// Set before an orderly metadata delete so a heartbeat cannot recreate the hash.
    pub stop_meta: Arc<watch::Sender<bool>>,
    pub announces: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Shared {
    fn note_worker(&self, queue: &str, worker_name: &str) {
        self.worker_names
            .lock()
            .expect("worker names")
            .push((queue.to_owned(), worker_name.to_owned()));
    }

    /// Delete each metadata hash from an orderly exit. A failed delete is logged.
    ///
    /// The tasks-done heartbeat is stopped and joined first, so it cannot
    /// recreate a hash this delete just removed.
    pub async fn delete_worker_metadata(&self) {
        let _ = self.stop_meta.send(true);
        let handles = std::mem::take(&mut *self.announces.lock().expect("announces"));
        for handle in handles {
            let _ = handle.await;
        }
        let noted = self.worker_names.lock().expect("worker names").clone();
        for (queue, name) in noted {
            let mut conn = self.redis.clone();
            if let Err(error) = delete_worker_metadata(&mut conn, &queue, &name).await {
                tracing::warn!(
                    %error,
                    %queue,
                    %name,
                    "failed to delete worker metadata"
                );
            }
        }
    }
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
        let backend = RedisStorage::<Job>::new(shared.redis.clone())
            .with_config(queue_config(&queue, &shared.config)?);
        let state = QueueState {
            shared: shared.clone(),
        };
        let pool = shared.pool.clone();
        let worker_name = worker_name(&queue);
        shared.note_worker(&queue, &worker_name);
        let shared_for_event = shared.clone();
        let queue_for_event = queue.clone();

        monitor = monitor.register(move |_| {
            let pool = pool.clone();
            let shared_for_event = shared_for_event.clone();
            let queue_for_event = queue_for_event.clone();
            let announced = worker_name.clone();
            WorkerBuilder::new(worker_name.clone())
                .backend(backend.clone())
                .data(state.clone())
                .concurrency(concurrency)
                .on_event(move |worker, event| {
                    if matches!(event, Event::Start) {
                        pool.remember(worker);
                        let shared = shared_for_event.clone();
                        let queue_name = queue_for_event.clone();
                        let name = announced.clone();
                        let stop = shared_for_event.stop_meta.subscribe();
                        let handle = tokio::spawn(async move {
                            if let Err(error) =
                                announce_worker(shared, queue_name, name, stop).await
                            {
                                tracing::warn!(%error, "failed to write worker metadata");
                            }
                        });
                        shared_for_event
                            .announces
                            .lock()
                            .expect("announces")
                            .push(handle);
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

async fn announce_worker(
    shared: Shared,
    queue: String,
    name: String,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let ppid = std::os::unix::process::parent_id();
    let slot = std::env::var("SIMPLE_Q_SLOT")
        .ok()
        .and_then(|raw| raw.parse().ok());
    let mut conn = shared.redis.clone();
    simple_q_runtime::admin::note_worker_start(
        &mut conn,
        &shared.config,
        &queue,
        &name,
        ppid,
        slot,
        shared.max_tasks,
    )
    .await?;
    let period = std::time::Duration::from_secs(shared.config.heartbeat_secs.max(1));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if *stop.borrow() {
                    return Ok(());
                }
                let done = shared.completed.load(Ordering::Relaxed);
                if let Err(error) = simple_q_runtime::admin::note_tasks_done(
                    &mut conn,
                    &shared.config,
                    &queue,
                    &name,
                    done,
                )
                .await
                {
                    tracing::warn!(%error, %name, "failed to write tasks_done");
                }
                if *stop.borrow() {
                    return Ok(());
                }
            }
            _ = stop.changed() => return Ok(()),
        }
    }
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
        if let Err(hook_error) = bridge::note_dead(&job, "invalid", 0, None).await {
            tracing::warn!(
                message_id = %job.message_id,
                actor_name = %job.actor_name,
                error = %hook_error,
                "dead-letter hook failed for an invalid message"
            );
        }
        release_unique_key(&state.shared.redis, &job).await;
        return Err(Box::new(AbortError::new(error)));
    }

    state.shared.pool.remember(&worker);
    if state.shared.draining.load(Ordering::SeqCst) || state.shared.shutdown.load(Ordering::SeqCst)
    {
        state.shared.pool.pause_all();
    }

    let task_id = task.task_id().to_owned();
    let mut conn = state.shared.redis.clone();
    let deliveries = match redis_api::incr_delivery(&mut conn, &job.message_id, job.retries).await {
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
        if let Err(hook_error) = bridge::note_dead(&job, "poison", deliveries, None).await {
            tracing::warn!(
                message_id = %job.message_id,
                actor_name = %job.actor_name,
                error = %hook_error,
                "dead-letter hook failed for a poison message"
            );
        }
        bridge::note_reject(&job.queue, &job.actor_name);
        note_finished(&state);
        release_unique_key(&state.shared.redis, &job).await;
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
    let result = bridge::execute(&dispatched, &task_id, deliveries, worker.name()).await;

    note_finished(&state);

    let value = match result {
        Ok(value) => value,
        Err(error) => {
            release_unique_key(&state.shared.redis, &job).await;
            return Err(Box::new(AbortError::new(error)));
        }
    };
    match value.get("outcome").and_then(|item| item.as_str()) {
        Some("retry") => {
            let delay_ms = value
                .get("delay_ms")
                .and_then(|item| item.as_u64())
                .unwrap_or(0);
            let mut next = job.clone();
            next.retries = next.retries.saturating_add(1);
            if let Err(error) = redis_api::enqueue_job(
                &mut state.shared.redis.clone(),
                &state.shared.config,
                next,
                Some(std::time::Duration::from_millis(delay_ms)),
            )
            .await
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
            release_acked(&state.shared.redis, &job).await;
            Ok(value)
        }
        Some("dead") => {
            release_unique_key(&state.shared.redis, &job).await;
            Err(Box::new(AbortError::new(anyhow::anyhow!(
                "dead: {}",
                value
                    .get("reason")
                    .and_then(|item| item.as_str())
                    .unwrap_or("dead")
            ))))
        }
        _ => {
            release_acked(&state.shared.redis, &job).await;
            release_unique_key(&state.shared.redis, &job).await;
            Ok(value)
        }
    }
}

async fn release_unique_key(redis: &ConnectionManager, job: &Job) {
    let Some(unique_key) = job.unique_key.as_deref().filter(|key| !key.is_empty()) else {
        return;
    };
    if let Err(error) =
        redis_api::release_idempotency(&mut redis.clone(), &job.queue, unique_key).await
    {
        tracing::warn!(
            message_id = %job.message_id,
            queue_name = %job.queue,
            error = %error,
            "failed to drop the idempotency key after a terminal outcome"
        );
    }
}

async fn release_acked(redis: &ConnectionManager, job: &Job) {
    if let Err(error) =
        redis_api::release_delivery(&mut redis.clone(), &job.message_id, job.retries).await
    {
        tracing::warn!(
            message_id = %job.message_id,
            retries = job.retries,
            error = %error,
            "failed to drop the delivery key after ack"
        );
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
