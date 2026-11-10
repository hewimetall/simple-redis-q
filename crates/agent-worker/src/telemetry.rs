//! OpenTelemetry bridge for the child process.
//!
//! ReadableSpan field mapping and W3C traceparent extraction follow the ideas
//! in pyo3-tracing-opentelemetry 0.3.3 (MIT OR Apache-2.0), specifically
//! `export.rs` and traceparent extraction. This module does not depend on that
//! crate. The Python exporter stays on one OS thread, this process does not
//! install a global tracer-provider subscriber by itself, and the Python
//! resource captured at startup is attached to every exported span.
//!
//! Tokio threads only enqueue finished spans. `Python::attach` for export runs
//! on the batch thread. `export_timeout` is the budget for one `export` call.
//! CPython cannot be interrupted, so a call that does not return holds that
//! thread until it does; spans that arrive while the queue is full are dropped.
//! Shutdown joins the thread so returned batches are in the Python exporter
//! before the application's `on_stop` hook. Exit code 2 does not join it.
//!
//! Call order: `read_tracing` checks the Python object, `install` builds the
//! text layer and the OpenTelemetry layer, the batch thread calls `export`,
//! `shutdown` joins that thread. `parent_context` and `with_span_traceparent`
//! belong to the process span, not to the exporter.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{SpanContext, SpanId, SpanKind, Status, TraceContextExt};
use opentelemetry::{Context, Value};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, Span, SpanData, SpanProcessor};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use simple_q_runtime::job::Job;

static PYTHON_EXPORT_CALLS: AtomicU64 = AtomicU64::new(0);
static SUBSCRIBER_INSTALLED: Mutex<bool> = Mutex::new(false);
static PROVIDER: Mutex<Option<ProviderHandle>> = Mutex::new(None);

#[derive(Debug)]
pub struct TracingConfigError {
    message: String,
}

impl std::fmt::Display for TracingConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TracingConfigError {}

fn config(message: impl Into<String>) -> TracingConfigError {
    TracingConfigError {
        message: message.into(),
    }
}

pub(crate) fn is_config_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<TracingConfigError>().is_some()
}

#[cfg(test)]
fn python_export_calls() -> u64 {
    PYTHON_EXPORT_CALLS.load(Ordering::Relaxed)
}

/// Values copied off the imported registry before validation.
pub struct RawTracing {
    pub exporter: Py<PyAny>,
    pub resource: Py<PyAny>,
    pub targets: Vec<(String, String)>,
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub scheduled_delay: Duration,
    pub export_timeout: Duration,
}

struct PythonClasses {
    span_context: Py<PyAny>,
    span_kind: Py<PyAny>,
    trace_flags: Py<PyAny>,
    trace_state: Py<PyAny>,
    readable_span: Py<PyAny>,
    event: Py<PyAny>,
    link: Py<PyAny>,
    instrumentation_scope: Py<PyAny>,
    status: Py<PyAny>,
    status_code: Py<PyAny>,
}

struct ParsedTracing {
    exporter: Py<PyAny>,
    resource: Py<PyAny>,
    classes: PythonClasses,
    targets: Targets,
    max_queue_size: usize,
    max_export_batch_size: usize,
    scheduled_delay: Duration,
    export_timeout: Duration,
}

struct ProviderHandle {
    provider: SdkTracerProvider,
    export_timeout: Duration,
    scheduled_delay: Duration,
}

/// Text log filter from `RUST_LOG`, defaulting to info. It is not applied to
/// the OpenTelemetry layer.
pub(crate) fn fmt_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

fn otel_targets(targets: &[(String, String)]) -> Result<Targets, TracingConfigError> {
    if targets.is_empty() {
        return Err(config(
            "tracing targets must name at least one crate; an empty filter exports nothing",
        ));
    }
    let mut parsed = Targets::new();
    for (target, level) in targets {
        let level: LevelFilter = level.parse().map_err(|_| {
            config(format!(
                "invalid tracing level `{level}` for target `{target}`"
            ))
        })?;
        parsed = parsed.with_target(target.clone(), level);
    }
    Ok(parsed)
}

pub fn read_tracing(
    _py: Python<'_>,
    app: &Bound<'_, PyAny>,
) -> Result<Option<RawTracing>, TracingConfigError> {
    let tracing = match app.getattr("tracing") {
        Ok(value) if !value.is_none() => value,
        _ => return Ok(None),
    };
    let exporter = tracing
        .getattr("exporter")
        .map_err(|_| config("tracing exporter is missing"))?;
    let resource = tracing
        .getattr("resource")
        .map_err(|_| config("tracing resource is missing"))?;
    let targets_obj = tracing
        .getattr("targets")
        .map_err(|_| config("tracing targets are missing"))?;
    let targets_dict = targets_obj
        .cast::<PyDict>()
        .map_err(|_| config("tracing targets must be a dict of target to level"))?;
    let mut targets = Vec::new();
    for (key, value) in targets_dict.iter() {
        let target: String = key
            .extract()
            .map_err(|_| config("tracing target names must be strings"))?;
        let level: String = value
            .extract()
            .map_err(|_| config(format!("tracing level for `{target}` must be a string")))?;
        targets.push((target, level));
    }
    let max_queue_size = positive_usize(&tracing, "max_queue_size")?;
    let max_export_batch_size = positive_usize(&tracing, "max_export_batch_size")?;
    let scheduled_delay_ms = positive_u64(&tracing, "scheduled_delay_ms")?;
    let export_timeout_ms = positive_u64(&tracing, "export_timeout_ms")?;
    Ok(Some(RawTracing {
        exporter: exporter.unbind(),
        resource: resource.unbind(),
        targets,
        max_queue_size,
        max_export_batch_size,
        scheduled_delay: Duration::from_millis(scheduled_delay_ms),
        export_timeout: Duration::from_millis(export_timeout_ms),
    }))
}

fn positive_usize(tracing: &Bound<'_, PyAny>, name: &str) -> Result<usize, TracingConfigError> {
    let value: usize = tracing
        .getattr(name)
        .and_then(|item| item.extract())
        .map_err(|_| config(format!("tracing {name} must be a positive integer")))?;
    if value == 0 {
        return Err(config(format!("tracing {name} must be greater than zero")));
    }
    Ok(value)
}

fn positive_u64(tracing: &Bound<'_, PyAny>, name: &str) -> Result<u64, TracingConfigError> {
    let value: u64 = tracing
        .getattr(name)
        .and_then(|item| item.extract())
        .map_err(|_| config(format!("tracing {name} must be a positive integer")))?;
    if value == 0 {
        return Err(config(format!("tracing {name} must be greater than zero")));
    }
    Ok(value)
}

fn parse_tracing(py: Python<'_>, raw: &RawTracing) -> Result<ParsedTracing, TracingConfigError> {
    require_callable(raw.exporter.bind(py), "export")?;
    require_callable(raw.exporter.bind(py), "shutdown")?;
    if raw.max_export_batch_size > raw.max_queue_size {
        return Err(config(
            "max_export_batch_size must be less than or equal to max_queue_size",
        ));
    }
    if raw.max_queue_size == 0 || raw.max_export_batch_size == 0 {
        return Err(config(
            "max_queue_size and max_export_batch_size must be greater than zero",
        ));
    }
    if raw.scheduled_delay.is_zero() || raw.export_timeout.is_zero() {
        return Err(config(
            "scheduled_delay_ms and export_timeout_ms must be greater than zero",
        ));
    }
    let targets = otel_targets(&raw.targets)?;
    require_otel_resource(py, raw.resource.bind(py))?;
    let classes = load_python_classes(py)?;
    Ok(ParsedTracing {
        exporter: raw.exporter.clone_ref(py),
        resource: raw.resource.clone_ref(py),
        classes,
        targets,
        max_queue_size: raw.max_queue_size,
        max_export_batch_size: raw.max_export_batch_size,
        scheduled_delay: raw.scheduled_delay,
        export_timeout: raw.export_timeout,
    })
}

fn require_callable(exporter: &Bound<'_, PyAny>, name: &str) -> Result<(), TracingConfigError> {
    let method = exporter
        .getattr(name)
        .map_err(|_| config(format!("exporter must have callable {name}")))?;
    if !method.is_callable() {
        return Err(config(format!("exporter must have callable {name}")));
    }
    Ok(())
}

fn require_otel_resource(
    py: Python<'_>,
    resource: &Bound<'_, PyAny>,
) -> Result<(), TracingConfigError> {
    let class = py
        .import("opentelemetry.sdk.resources")
        .and_then(|module| module.getattr("Resource"))
        .map_err(|_| config("resource must be an OpenTelemetry Resource"))?;
    let matches = resource
        .is_instance(&class)
        .map_err(|_| config("resource must be an OpenTelemetry Resource"))?;
    if !matches {
        return Err(config("resource must be an OpenTelemetry Resource"));
    }
    Ok(())
}

fn load_python_classes(py: Python<'_>) -> Result<PythonClasses, TracingConfigError> {
    let trace = py
        .import("opentelemetry.trace")
        .map_err(|_| config("resource must be an OpenTelemetry Resource"))?;
    let sdk_trace = py
        .import("opentelemetry.sdk.trace")
        .map_err(|_| config("resource must be an OpenTelemetry Resource"))?;
    let instrumentation = py
        .import("opentelemetry.sdk.util.instrumentation")
        .map_err(|_| config("resource must be an OpenTelemetry Resource"))?;
    let status = py
        .import("opentelemetry.trace.status")
        .map_err(|_| config("resource must be an OpenTelemetry Resource"))?;
    Ok(PythonClasses {
        span_context: trace
            .getattr("SpanContext")
            .map_err(|_| config("OpenTelemetry SpanContext is not available"))?
            .unbind(),
        span_kind: trace
            .getattr("SpanKind")
            .map_err(|_| config("OpenTelemetry SpanKind is not available"))?
            .unbind(),
        trace_flags: trace
            .getattr("TraceFlags")
            .map_err(|_| config("OpenTelemetry TraceFlags is not available"))?
            .unbind(),
        trace_state: trace
            .getattr("TraceState")
            .map_err(|_| config("OpenTelemetry TraceState is not available"))?
            .unbind(),
        readable_span: sdk_trace
            .getattr("ReadableSpan")
            .map_err(|_| config("OpenTelemetry ReadableSpan is not available"))?
            .unbind(),
        event: sdk_trace
            .getattr("Event")
            .map_err(|_| config("OpenTelemetry Event is not available"))?
            .unbind(),
        link: trace
            .getattr("Link")
            .map_err(|_| config("OpenTelemetry Link is not available"))?
            .unbind(),
        instrumentation_scope: instrumentation
            .getattr("InstrumentationScope")
            .map_err(|_| config("OpenTelemetry InstrumentationScope is not available"))?
            .unbind(),
        status: status
            .getattr("Status")
            .map_err(|_| config("OpenTelemetry Status is not available"))?
            .unbind(),
        status_code: status
            .getattr("StatusCode")
            .map_err(|_| config("OpenTelemetry StatusCode is not available"))?
            .unbind(),
    })
}

pub fn install(cfg: Option<RawTracing>) -> Result<()> {
    let Some(raw) = cfg else {
        install_fmt_only();
        return Ok(());
    };
    let parsed = Python::attach(|py| parse_tracing(py, &raw)).map_err(anyhow::Error::new)?;
    install_otel(parsed).map_err(anyhow::Error::new)?;
    Ok(())
}

fn install_fmt_only() {
    let mut installed = SUBSCRIBER_INSTALLED.lock().expect("subscriber");
    if *installed {
        return;
    }
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(fmt_env_filter());
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
    *installed = true;
}

fn install_otel(parsed: ParsedTracing) -> Result<(), TracingConfigError> {
    let export_timeout = parsed.export_timeout;
    let scheduled_delay = parsed.scheduled_delay;
    let targets = parsed.targets.clone();
    let processor = PythonBatchProcessor::start(parsed)?;
    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();
    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "agent-worker");
    let otel_layer = OpenTelemetryLayer::new(tracer).with_filter(targets);
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(fmt_env_filter());

    let mut installed = SUBSCRIBER_INSTALLED.lock().expect("subscriber");
    if !*installed {
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel_layer)
            .try_init()
            .map_err(|error| {
                config(format!("failed to install the tracing subscriber: {error}"))
            })?;
        *installed = true;
    }
    *PROVIDER.lock().expect("provider") = Some(ProviderHandle {
        provider,
        export_timeout,
        scheduled_delay,
    });
    Ok(())
}

pub async fn shutdown() {
    let handle = PROVIDER.lock().expect("provider").take();
    let Some(handle) = handle else {
        return;
    };
    let timeout = handle.export_timeout.saturating_add(handle.scheduled_delay);
    let _ =
        tokio::task::spawn_blocking(move || handle.provider.shutdown_with_timeout(timeout)).await;
}

struct HeaderCarrier<'a>(&'a BTreeMap<String, String>);

impl Extractor for HeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str).or_else(|| {
            self.0
                .iter()
                .find_map(|(name, value)| name.eq_ignore_ascii_case(key).then_some(value.as_str()))
        })
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Parent context from a W3C `traceparent` header, plus `tracestate` when present.
pub fn parent_context(headers: &BTreeMap<String, String>) -> Option<Context> {
    let mut carrier = HashMap::new();
    let traceparent = HeaderCarrier(headers).get("traceparent")?.to_owned();
    carrier.insert("traceparent".to_owned(), traceparent);
    if let Some(tracestate) = HeaderCarrier(headers).get("tracestate") {
        carrier.insert("tracestate".to_owned(), tracestate.to_owned());
    }
    let context = TraceContextPropagator::new().extract(&carrier);
    context.span().span_context().is_valid().then_some(context)
}

/// Optional span link carried as a W3C traceparent in the `tracelink` header.
pub fn link_context(headers: &BTreeMap<String, String>) -> Option<SpanContext> {
    let traceparent = headers.get("tracelink")?;
    let carrier = HashMap::from([("traceparent".to_owned(), traceparent.clone())]);
    let context = TraceContextPropagator::new().extract(&carrier);
    let span_context = context.span().span_context().clone();
    span_context.is_valid().then_some(span_context)
}

/// Copy of `job` whose trace headers describe `span`. The original job is unchanged.
pub fn with_span_traceparent(job: &Job, span: &tracing::Span) -> Job {
    let mut copy = job.clone();
    let mut carrier = HashMap::new();
    TraceContextPropagator::new().inject_context(&span.context(), &mut carrier);
    if let Some(traceparent) = carrier.get("traceparent") {
        copy.headers
            .insert("traceparent".to_owned(), traceparent.clone());
    }
    if let Some(tracestate) = carrier.get("tracestate").filter(|value| !value.is_empty()) {
        copy.headers
            .insert("tracestate".to_owned(), tracestate.clone());
    }
    copy
}

struct QueueState {
    queue: VecDeque<SpanData>,
    shutdown: bool,
    flush: bool,
    exporting: bool,
    max_queue_size: usize,
}

struct Shared {
    state: Mutex<QueueState>,
    wake: Condvar,
    exporter: Py<PyAny>,
    resource: Py<PyAny>,
    classes: PythonClasses,
    max_export_batch_size: usize,
    scheduled_delay: Duration,
    export_timeout: Duration,
}

struct PythonBatchProcessor {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
    exporter: Py<PyAny>,
}

impl PythonBatchProcessor {
    fn start(parsed: ParsedTracing) -> Result<Self, TracingConfigError> {
        let shutdown_exporter = Python::attach(|py| parsed.exporter.clone_ref(py));
        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                queue: VecDeque::new(),
                shutdown: false,
                flush: false,
                exporting: false,
                max_queue_size: parsed.max_queue_size,
            }),
            wake: Condvar::new(),
            exporter: parsed.exporter,
            resource: parsed.resource,
            classes: parsed.classes,
            max_export_batch_size: parsed.max_export_batch_size,
            scheduled_delay: parsed.scheduled_delay,
            export_timeout: parsed.export_timeout,
        });
        let thread_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("otel-export".to_owned())
            .spawn(move || batch_loop(thread_shared))
            .map_err(|error| config(format!("failed to start the export thread: {error}")))?;
        Ok(Self {
            shared,
            thread: Mutex::new(Some(thread)),
            exporter: shutdown_exporter,
        })
    }
}

impl std::fmt::Debug for PythonBatchProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonBatchProcessor")
            .finish_non_exhaustive()
    }
}

impl SpanProcessor for PythonBatchProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        let mut state = self.shared.state.lock().expect("otel queue");
        if state.shutdown || state.queue.len() >= state.max_queue_size {
            return;
        }
        state.queue.push_back(span);
        self.shared.wake.notify_one();
    }

    fn force_flush(&self) -> OTelSdkResult {
        let mut state = self.shared.state.lock().expect("otel queue");
        if state.shutdown {
            return Ok(());
        }
        state.flush = true;
        self.shared.wake.notify_one();
        while state.flush || state.exporting {
            state = self.shared.wake.wait(state).expect("otel queue");
        }
        Ok(())
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        {
            let mut state = self.shared.state.lock().expect("otel queue");
            if state.shutdown {
                return Err(OTelSdkError::AlreadyShutdown);
            }
            state.shutdown = true;
            state.flush = true;
            self.shared.wake.notify_one();
        }
        let handle = self.thread.lock().expect("otel thread").take();
        if let Some(handle) = handle {
            if !join_for(handle, timeout) {
                return Err(OTelSdkError::Timeout(timeout));
            }
        }
        python_shutdown(&self.exporter)
    }
}

fn join_for(handle: JoinHandle<()>, timeout: Duration) -> bool {
    let (sender, receiver) = std::sync::mpsc::channel();
    let _ = std::thread::Builder::new()
        .name("otel-shutdown-wait".to_owned())
        .spawn(move || {
            let _ = handle.join();
            let _ = sender.send(());
        });
    receiver.recv_timeout(timeout).is_ok()
}

fn python_shutdown(exporter: &Py<PyAny>) -> OTelSdkResult {
    let result = Python::attach(|py| -> PyResult<()> {
        exporter.bind(py).call_method0("shutdown")?;
        Ok(())
    });
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(OTelSdkError::InternalFailure(error.to_string())),
    }
}

fn batch_loop(shared: Arc<Shared>) {
    let mut started: Option<Instant> = None;
    loop {
        let batch = {
            let mut state = shared.state.lock().expect("otel queue");
            loop {
                if state.queue.is_empty() {
                    started = None;
                    if state.flush {
                        state.flush = false;
                        state.exporting = false;
                        shared.wake.notify_all();
                    }
                    if state.shutdown {
                        return;
                    }
                    state.exporting = false;
                    state = shared.wake.wait(state).expect("otel queue");
                    continue;
                }
                if started.is_none() {
                    started = Some(Instant::now());
                }
                let elapsed = started.expect("batch timer").elapsed();
                let ready = state.shutdown
                    || state.flush
                    || state.queue.len() >= shared.max_export_batch_size
                    || elapsed >= shared.scheduled_delay;
                if !ready {
                    let remaining = shared.scheduled_delay.saturating_sub(elapsed);
                    let (guard, _) = shared
                        .wake
                        .wait_timeout(state, remaining)
                        .expect("otel queue");
                    state = guard;
                    continue;
                }
                let count = shared.max_export_batch_size.min(state.queue.len());
                let batch: Vec<SpanData> = state.queue.drain(..count).collect();
                if state.queue.is_empty() {
                    started = None;
                } else {
                    started = Some(Instant::now());
                }
                state.exporting = true;
                break batch;
            }
        };
        let began = Instant::now();
        if let Err(error) = export_batch(&shared, &batch) {
            tracing::warn!(%error, "span export failed");
        } else if began.elapsed() > shared.export_timeout {
            tracing::warn!(
                elapsed_ms = began.elapsed().as_millis() as u64,
                timeout_ms = shared.export_timeout.as_millis() as u64,
                "span export exceeded export_timeout"
            );
        }
        let mut state = shared.state.lock().expect("otel queue");
        state.exporting = false;
        if state.queue.is_empty() {
            state.flush = false;
        }
        shared.wake.notify_all();
        if state.shutdown && state.queue.is_empty() {
            return;
        }
    }
}

fn export_batch(shared: &Shared, batch: &[SpanData]) -> PyResult<()> {
    Python::attach(|py| {
        PYTHON_EXPORT_CALLS.fetch_add(1, Ordering::Relaxed);
        let spans = PyList::empty(py);
        for span in batch {
            let readable =
                span_data_to_readable_span(py, span, shared.resource.bind(py), &shared.classes)?;
            spans.append(readable)?;
        }
        shared.exporter.bind(py).call_method1("export", (spans,))?;
        Ok(())
    })
}

fn system_time_to_nanos(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0)
}

fn trace_id_to_u128(trace_id: opentelemetry::trace::TraceId) -> u128 {
    u128::from_be_bytes(trace_id.to_bytes())
}

fn span_id_to_u64(span_id: SpanId) -> u64 {
    u64::from_be_bytes(span_id.to_bytes())
}

fn value_to_pyobject(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Bool(item) => Ok((*item).into_py_any(py)?),
        Value::I64(item) => Ok((*item).into_py_any(py)?),
        Value::F64(item) => Ok((*item).into_py_any(py)?),
        Value::String(item) => Ok(item.as_str().into_py_any(py)?),
        Value::Array(array) => match array {
            opentelemetry::Array::Bool(items) => Ok(items.clone().into_py_any(py)?),
            opentelemetry::Array::I64(items) => Ok(items.clone().into_py_any(py)?),
            opentelemetry::Array::F64(items) => Ok(items.clone().into_py_any(py)?),
            opentelemetry::Array::String(items) => {
                let strings: Vec<&str> = items.iter().map(|item| item.as_str()).collect();
                Ok(strings.into_py_any(py)?)
            }
            _ => Ok(format!("{array:?}").into_py_any(py)?),
        },
        other => Ok(format!("{other:?}").into_py_any(py)?),
    }
}

fn trace_state_to_python<'py>(
    trace_state: &opentelemetry::trace::TraceState,
    trace_state_class: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let header = trace_state.header();
    let pairs: Vec<(String, String)> = header
        .split(',')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) if !key.is_empty() => {
                    Some((key.to_owned(), value.to_owned()))
                }
                _ => None,
            }
        })
        .collect();
    if pairs.is_empty() {
        trace_state_class.call0()
    } else {
        trace_state_class.call1((pairs,))
    }
}

fn span_context<'py>(
    py: Python<'py>,
    classes: &PythonClasses,
    trace_id: u128,
    span_id: u64,
    is_remote: bool,
    flags: &Bound<'py, PyAny>,
    trace_state: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    classes
        .span_context
        .bind(py)
        .call1((trace_id, span_id, is_remote, flags, trace_state))
}

/// Convert `SpanData` to a Python `ReadableSpan`.
///
/// The resource object is the one captured at startup. Links are copied from
/// the Rust span; they are not replaced with an empty list.
fn span_data_to_readable_span<'py>(
    py: Python<'py>,
    span: &SpanData,
    resource: &Bound<'py, PyAny>,
    classes: &PythonClasses,
) -> PyResult<Bound<'py, PyAny>> {
    let trace_flags = classes
        .trace_flags
        .bind(py)
        .call1((span.span_context.trace_flags().to_u8(),))?;
    let trace_state = trace_state_to_python(
        span.span_context.trace_state(),
        classes.trace_state.bind(py),
    )?;
    let trace_id = trace_id_to_u128(span.span_context.trace_id());
    let span_id = span_id_to_u64(span.span_context.span_id());
    let parent_span_id = span_id_to_u64(span.parent_span_id);
    let context = span_context(
        py,
        classes,
        trace_id,
        span_id,
        false,
        &trace_flags,
        &trace_state,
    )?;
    let parent = if parent_span_id != 0 {
        Some(span_context(
            py,
            classes,
            trace_id,
            parent_span_id,
            span.parent_span_is_remote,
            &trace_flags,
            &trace_state,
        )?)
    } else {
        None
    };

    let scope = classes.instrumentation_scope.bind(py).call1((
        span.instrumentation_scope.name(),
        span.instrumentation_scope.version(),
    ))?;
    let (status_code, status_description): (Bound<'_, PyAny>, Option<&str>) = match &span.status {
        Status::Unset => (classes.status_code.bind(py).getattr("UNSET")?, None),
        Status::Ok => (classes.status_code.bind(py).getattr("OK")?, None),
        Status::Error { description } => (
            classes.status_code.bind(py).getattr("ERROR")?,
            Some(description.as_ref()),
        ),
    };
    let status = classes
        .status
        .bind(py)
        .call1((&status_code, status_description))?;
    let kind = match span.span_kind {
        SpanKind::Internal => classes.span_kind.bind(py).getattr("INTERNAL")?,
        SpanKind::Server => classes.span_kind.bind(py).getattr("SERVER")?,
        SpanKind::Client => classes.span_kind.bind(py).getattr("CLIENT")?,
        SpanKind::Producer => classes.span_kind.bind(py).getattr("PRODUCER")?,
        SpanKind::Consumer => classes.span_kind.bind(py).getattr("CONSUMER")?,
    };
    let attributes = PyDict::new(py);
    for item in &span.attributes {
        attributes.set_item(item.key.as_str(), value_to_pyobject(py, &item.value)?)?;
    }
    let events = PyList::empty(py);
    for event in &span.events.events {
        let event_attributes = PyDict::new(py);
        for item in &event.attributes {
            event_attributes.set_item(item.key.as_str(), value_to_pyobject(py, &item.value)?)?;
        }
        let py_event = classes.event.bind(py).call1((
            event.name.as_ref(),
            event_attributes,
            system_time_to_nanos(event.timestamp),
        ))?;
        events.append(py_event)?;
    }
    let links = PyList::empty(py);
    for link in &span.links.links {
        let link_flags = classes
            .trace_flags
            .bind(py)
            .call1((link.span_context.trace_flags().to_u8(),))?;
        let link_state = trace_state_to_python(
            link.span_context.trace_state(),
            classes.trace_state.bind(py),
        )?;
        let link_context = span_context(
            py,
            classes,
            trace_id_to_u128(link.span_context.trace_id()),
            span_id_to_u64(link.span_context.span_id()),
            link.span_context.is_remote(),
            &link_flags,
            &link_state,
        )?;
        let link_attributes = PyDict::new(py);
        for item in &link.attributes {
            link_attributes.set_item(item.key.as_str(), value_to_pyobject(py, &item.value)?)?;
        }
        let py_link = classes
            .link
            .bind(py)
            .call1((link_context, link_attributes))?;
        links.append(py_link)?;
    }

    let kwargs = PyDict::new(py);
    kwargs.set_item("name", span.name.as_ref())?;
    kwargs.set_item("context", context)?;
    kwargs.set_item("parent", parent)?;
    kwargs.set_item("resource", resource)?;
    kwargs.set_item("attributes", attributes)?;
    kwargs.set_item("events", events)?;
    kwargs.set_item("links", links)?;
    kwargs.set_item("kind", kind)?;
    kwargs.set_item("status", status)?;
    kwargs.set_item("start_time", system_time_to_nanos(span.start_time))?;
    kwargs.set_item("end_time", system_time_to_nanos(span.end_time))?;
    kwargs.set_item("instrumentation_scope", scope)?;
    classes.readable_span.bind(py).call((), Some(&kwargs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt;
    use tracing::Level;
    use tracing_subscriber::Layer;

    #[test]
    fn install_without_tracing_does_not_export() {
        let before = python_export_calls();
        install(None).unwrap();
        assert_eq!(python_export_calls(), before);
    }

    #[test]
    fn otel_targets_stay_independent_of_the_fmt_filter() {
        let fmt = EnvFilter::new("warn");
        let targets = otel_targets(&[
            ("agent_worker".into(), "info".into()),
            ("simple_q_runtime".into(), "info".into()),
            ("apalis".into(), "debug".into()),
        ])
        .unwrap();
        assert!(targets.would_enable("agent_worker", &Level::INFO));
        assert!(targets.would_enable("agent_worker::worker", &Level::INFO));
        assert!(targets.would_enable("simple_q_runtime", &Level::INFO));
        assert!(targets.would_enable("apalis", &Level::DEBUG));
        assert!(!targets.would_enable("other_target", &Level::INFO));
        let _ = fmt;
    }

    #[test]
    fn warn_fmt_filter_still_exports_an_info_span() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let processor = MemoryProcessor {
            names: Arc::clone(&names),
        };
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .build();
        let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "agent-worker");
        let targets = otel_targets(&[("agent_worker".into(), "info".into())]).unwrap();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::new("warn")))
            .with(OpenTelemetryLayer::new(tracer).with_filter(targets));
        tracing::subscriber::with_default(subscriber, || {
            {
                let span = tracing::info_span!(target: "agent_worker", "kept");
                let _entered = span.enter();
            }
            {
                let span = tracing::info_span!(target: "other_target", "skipped");
                let _entered = span.enter();
            }
        });
        let _ = provider.shutdown();
        let recorded = names.lock().expect("names").clone();
        assert!(
            recorded.iter().any(|name| name == "kept"),
            "otel layer dropped the info span under RUST_LOG=warn: {recorded:?}"
        );
        assert!(
            recorded.iter().all(|name| name != "skipped"),
            "unlisted target was exported: {recorded:?}"
        );
    }

    #[test]
    fn traceparent_extraction_keeps_flags_and_tracestate() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "traceparent".into(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03".into(),
        );
        headers.insert("tracestate".into(), "vendor=opaque-value,other=123".into());
        let context = parent_context(&headers).expect("traceparent");
        let span_context = context.span().span_context().clone();
        assert!(span_context.is_valid());
        assert!(span_context.is_remote());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert!(span_context.trace_flags().is_sampled());
        assert_eq!(
            span_context.trace_state().header(),
            "vendor=opaque-value,other=123"
        );

        headers.insert(
            "traceparent".into(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02".into(),
        );
        let unsampled = parent_context(&headers).expect("unsampled traceparent");
        assert!(!unsampled.span().span_context().trace_flags().is_sampled());
    }

    #[test]
    fn invalid_traceparent_is_not_a_parent() {
        for value in [
            "invalid",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        ] {
            let mut headers = BTreeMap::new();
            headers.insert("traceparent".into(), value.into());
            assert!(parent_context(&headers).is_none(), "{value}");
        }
        assert!(parent_context(&BTreeMap::new()).is_none());
    }

    #[test]
    fn parser_rejects_exporter_resource_and_targets() {
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            py.run(
                cr#"
class Exporter:
    def export(self, spans):
        return None
    def shutdown(self):
        return None

class NotCallable:
    export = "no"
    shutdown = "no"

exporter = Exporter()
bad_exporter = NotCallable()
"#,
                None,
                Some(&locals),
            )
            .unwrap();
            let exporter: Py<PyAny> = locals.get_item("exporter").unwrap().unwrap().unbind();
            let bad_exporter: Py<PyAny> =
                locals.get_item("bad_exporter").unwrap().unwrap().unbind();
            let bad = RawTracing {
                exporter: bad_exporter,
                resource: py.None(),
                targets: vec![("agent_worker".into(), "info".into())],
                max_queue_size: 8,
                max_export_batch_size: 4,
                scheduled_delay: Duration::from_millis(10),
                export_timeout: Duration::from_millis(10),
            };
            let Err(error) = parse_tracing(py, &bad) else {
                panic!("exporter without callables was accepted");
            };
            assert!(error.to_string().contains("export"), "{error}");

            let bad_resource = RawTracing {
                exporter: exporter.clone_ref(py),
                resource: PyDict::new(py).into_any().unbind(),
                targets: vec![("agent_worker".into(), "info".into())],
                max_queue_size: 8,
                max_export_batch_size: 4,
                scheduled_delay: Duration::from_millis(10),
                export_timeout: Duration::from_millis(10),
            };
            let Err(error) = parse_tracing(py, &bad_resource) else {
                panic!("dict was accepted as a resource");
            };
            assert!(
                error.to_string().contains("OpenTelemetry Resource"),
                "{error}"
            );

            let bad_level = RawTracing {
                exporter,
                resource: py.None(),
                targets: vec![("agent_worker".into(), "verbose".into())],
                max_queue_size: 8,
                max_export_batch_size: 4,
                scheduled_delay: Duration::from_millis(10),
                export_timeout: Duration::from_millis(10),
            };
            let Err(error) = parse_tracing(py, &bad_level) else {
                panic!("invalid target level was accepted");
            };
            assert!(error.to_string().contains("verbose"), "{error}");

            let empty_targets = RawTracing {
                exporter: bad_level.exporter.clone_ref(py),
                resource: py.None(),
                targets: Vec::new(),
                max_queue_size: 8,
                max_export_batch_size: 4,
                scheduled_delay: Duration::from_millis(10),
                export_timeout: Duration::from_millis(10),
            };
            let Err(error) = parse_tracing(py, &empty_targets) else {
                panic!("empty targets were accepted");
            };
            assert!(error.to_string().contains("empty filter"), "{error}");
        });
    }

    #[derive(Clone)]
    struct MemoryProcessor {
        names: Arc<Mutex<Vec<String>>>,
    }

    impl std::fmt::Debug for MemoryProcessor {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MemoryProcessor").finish_non_exhaustive()
        }
    }

    impl SpanProcessor for MemoryProcessor {
        fn on_start(&self, _span: &mut Span, _cx: &Context) {}

        fn on_end(&self, span: SpanData) {
            self.names
                .lock()
                .expect("names")
                .push(span.name.to_string());
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }
    }
}
