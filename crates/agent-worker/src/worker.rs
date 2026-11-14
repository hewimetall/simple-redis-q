//! One delivery of one job.
//!
//! Poison is decided here, from the delivery count, before Python is called.
//! Retry and timeout come back from Python, because they need the exception.
//! Redis is the server side (`redis_api`). The handler sees a copy of the
//! message, including the delivery count and this process's worker name.
//! Retry stores the original job, so the sender's headers stay in Redis.
//! The delivery key is deleted only after ack. The idempotency key is
//! deleted only after Redis has stored the task in `done` or `dead`. The child
//! keeps checking until that write is visible, the process stops, or
//! `SIMPLE_Q_UNIQUE_TTL_SECS` elapses, and joins that task before it exits.
//! A retry leaves the key in place. A crash before that delete, including
//! `SIGKILL`, leaves it until the TTL or until the redelivered attempt finishes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use apalis::prelude::*;
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
use simple_q_runtime::redis_api;
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
    /// Idempotency deletes that must finish before this process exits.
    pub releases: Arc<Mutex<Vec<JoinHandle<()>>>>,
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
        join_tracked(&self.announces).await;
        join_tracked(&self.releases).await;
        let done = self.completed.load(Ordering::Relaxed);
        let mut conn = self.redis.clone();
        if let Err(error) = simple_q_runtime::admin::note_child_tasks(
            &mut conn,
            &simple_q_runtime::ids::hostname(),
            std::os::unix::process::parent_id(),
            std::process::id(),
            done,
        )
        .await
        {
            tracing::warn!(%error, "failed to record tasks_done on the supervisor");
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
    // A backend error used to end the monitor, and the child exited.
    // Apalis restarts the worker when this returns true. The factory then
    // uses a new name, so the old inflight set can be orphaned.
    let mut monitor = Monitor::new()
        .should_restart(|worker, error| restart_after_backend_error(worker.restarts(), error));

    for queue in queues {
        let backend = redis_api::worker_storage(shared.redis.clone(), &queue, &shared.config)?;
        let state = QueueState {
            shared: shared.clone(),
        };
        let pool = shared.pool.clone();
        let initial_name = worker_name(&queue);
        let generation_stop = Arc::new(Mutex::new(Arc::new(AtomicBool::new(false))));
        let shared_for_event = shared.clone();
        let queue_for_event = queue.clone();

        monitor = monitor.register(move |restarts| {
            let name = if restarts == 0 {
                initial_name.clone()
            } else {
                worker_name(&queue_for_event)
            };
            let stop_this = {
                let mut current = generation_stop.lock().expect("generation");
                current.store(true, Ordering::Relaxed);
                let next = Arc::new(AtomicBool::new(false));
                *current = next.clone();
                next
            };
            let pool = pool.clone();
            let shared_for_event = shared_for_event.clone();
            let queue_for_event = queue_for_event.clone();
            WorkerBuilder::new(name)
                .backend(backend.clone())
                .data(state.clone())
                .concurrency(concurrency)
                .on_event(move |worker, event| {
                    if matches!(event, Event::Start) {
                        pool.remember(worker);
                        let announced = worker.name().to_owned();
                        shared_for_event.note_worker(&queue_for_event, &announced);
                        let shared = shared_for_event.clone();
                        let queue_name = queue_for_event.clone();
                        let stop = shared_for_event.stop_meta.subscribe();
                        let stop_generation = stop_this.clone();
                        let handle = tokio::spawn(async move {
                            if let Err(error) = announce_worker(
                                shared,
                                queue_name,
                                announced,
                                stop,
                                stop_generation,
                            )
                            .await
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

/// A backend error restarts the worker until this many restarts have already run.
///
/// `Monitor::should_restart` sees the count before it increments. Eight leaves
/// room for a lost ack reply, which is a script error and is not replayed, and
/// still ends a Redis that never comes back.
const BACKEND_RESTARTS: usize = 8;

fn restart_after_backend_error(restarts: usize, error: &WorkerError) -> bool {
    restarts < BACKEND_RESTARTS && matches!(error, WorkerError::BackendError(_))
}

async fn announce_worker(
    shared: Shared,
    queue: String,
    name: String,
    mut stop: watch::Receiver<bool>,
    stop_generation: Arc<AtomicBool>,
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
                if *stop.borrow() || stop_generation.load(Ordering::Relaxed) {
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
                if let Err(error) = simple_q_runtime::admin::note_child_tasks(
                    &mut conn,
                    &simple_q_runtime::ids::hostname(),
                    std::os::unix::process::parent_id(),
                    std::process::id(),
                    done,
                )
                .await
                {
                    tracing::warn!(%error, %name, "failed to record tasks_done on the supervisor");
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
    let _busy = crate::slot_busy::enter();
    let task_id = task.task_id().to_owned();
    let mut conn = state.shared.redis.clone();
    if let Err(error) = simple_q_runtime::pages::note_delivery_start(
        &mut conn,
        &state.shared.config,
        &job.queue,
        &task_id,
    )
    .await
    {
        tracing::warn!(
            message_id = %job.message_id,
            actor_name = %job.actor_name,
            queue_name = %job.queue,
            %error,
            "failed to record started_at"
        );
    }

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
        record_dead(&mut conn, &state, &job, &task_id, "invalid", None, 0).await;
        release_unique_key_after_ack(&state.shared, &job, &task_id);
        return Err(Box::new(AbortError::new(error)));
    }

    state.shared.pool.remember(&worker);
    if state.shared.draining.load(Ordering::SeqCst) || state.shared.shutdown.load(Ordering::SeqCst)
    {
        state.shared.pool.pause_all();
    }

    let deliveries = match redis_api::incr_delivery(
        &mut conn,
        &job.message_id,
        job.retries,
        state.shared.config.dead_ttl_secs,
    )
    .await
    {
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
        record_dead(
            &mut conn, &state, &job, &task_id, "poison", None, deliveries,
        )
        .await;
        release_unique_key_after_ack(&state.shared, &job, &task_id);
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
            let text = error.to_string();
            record_dead(
                &mut conn,
                &state,
                &job,
                &task_id,
                "dispatch",
                Some(&text),
                deliveries,
            )
            .await;
            release_unique_key_after_ack(&state.shared, &job, &task_id);
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
            let reason = value
                .get("reason")
                .and_then(|item| item.as_str())
                .unwrap_or("dead");
            let error_text = value
                .get("error")
                .and_then(|item| item.as_str())
                .filter(|text| !text.is_empty());
            record_dead(
                &mut conn, &state, &job, &task_id, reason, error_text, deliveries,
            )
            .await;
            release_unique_key_after_ack(&state.shared, &job, &task_id);
            Err(Box::new(AbortError::new(anyhow::anyhow!("dead: {reason}"))))
        }
        _ => {
            release_acked(&state.shared.redis, &job).await;
            release_unique_key_after_ack(&state.shared, &job, &task_id);
            Ok(value)
        }
    }
}

async fn record_dead(
    conn: &mut ConnectionManager,
    state: &Data<QueueState>,
    job: &Job,
    task_id: &str,
    reason: &str,
    error: Option<&str>,
    deliveries: u64,
) {
    if let Err(record_error) = simple_q_runtime::pages::record_dead(
        conn,
        &state.shared.config,
        &job.queue,
        task_id,
        reason,
        error,
        deliveries,
    )
    .await
    {
        tracing::warn!(
            message_id = %job.message_id,
            actor_name = %job.actor_name,
            queue_name = %job.queue,
            %reason,
            error = %record_error,
            "failed to record a dead delivery"
        );
    }
}

fn release_unique_key_after_ack(shared: &Shared, job: &Job, task_id: &str) {
    let Some(unique_key) = job.unique_key.clone().filter(|key| !key.is_empty()) else {
        return;
    };
    let queue = job.queue.clone();
    let message_id = job.message_id.clone();
    let task_id = task_id.to_owned();
    let mut conn = shared.redis.clone();
    let budget = std::time::Duration::from_secs(shared.config.unique_ttl_secs.max(1));
    let wait = shared.config.idempotency_wait();
    let wait_max = shared.config.idempotency_wait_max();
    let mut stop = shared.stop_meta.subscribe();
    let handle = tokio::spawn(async move {
        // The handler returns before Apalis writes done or dead. Keep checking
        // until that write is visible. Stopping after a second leaves the key
        // until SIMPLE_Q_UNIQUE_TTL_SECS even though the ack landed late.
        match redis_api::release_idempotency_when_terminal(
            &mut conn,
            &queue,
            &task_id,
            &unique_key,
            &mut stop,
            budget,
            wait,
            wait_max,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    message_id = %message_id,
                    queue_name = %queue,
                    "left the idempotency key in place; the task was not terminal yet"
                );
            }
            Err(error) => {
                tracing::warn!(
                    message_id = %message_id,
                    queue_name = %queue,
                    error = %error,
                    "left the idempotency key in place"
                );
            }
        }
    });
    let mut releases = shared.releases.lock().expect("releases");
    releases.retain(|pending| !pending.is_finished());
    releases.push(handle);
}

async fn join_tracked(handles: &Mutex<Vec<JoinHandle<()>>>) {
    let pending = std::mem::take(&mut *handles.lock().expect("tracked tasks"));
    for handle in pending {
        let _ = handle.await;
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

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
