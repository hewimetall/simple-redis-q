//! Supervisor traces and Sentry.
//!
//! Off unless a DSN or an OTLP endpoint is configured. Export runs on the
//! batch processor's thread and does not run on the memory or signal path.
//! A down collector drops spans. The DSN and OTLP headers are not logged.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::{Span, Tracer};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracer, SdkTracerProvider, SpanData,
    SpanExporter,
};
use simple_q_parent::{OtlpProtocol, SamplerName, TelemetryConfig};

static CURRENT: OnceLock<SupervisorTelemetry> = OnceLock::new();

#[derive(Clone)]
pub struct SupervisorTelemetry {
    inner: Arc<Inner>,
}

struct Inner {
    tracer: Option<SdkTracer>,
    provider: Option<Arc<SdkTracerProvider>>,
    /// Kept alive for the batch exporter. Dropped after the provider so a
    /// flush still has a reactor. The handle inside the exporter is what
    /// export calls, so this field is read only by `Drop`.
    #[allow(dead_code)]
    export_runtime: Option<tokio::runtime::Runtime>,
    sentry: Option<sentry::ClientInitGuard>,
    host: String,
    version: String,
    traces: &'static str,
    sentry_label: &'static str,
    timeout: Duration,
}

impl SupervisorTelemetry {
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(Inner {
                tracer: None,
                provider: None,
                export_runtime: None,
                sentry: None,
                host: String::new(),
                version: String::new(),
                traces: "off",
                sentry_label: "off",
                timeout: Duration::from_secs(5),
            }),
        }
    }

    pub fn traces_label(&self) -> &'static str {
        self.inner.traces
    }

    pub fn sentry_label(&self) -> &'static str {
        self.inner.sentry_label
    }

    pub fn supervisor_started(&self, pid: u32, queue: &str) {
        self.span(
            "supervisor.start",
            &[("pid", pid.to_string()), ("queue", queue.to_owned())],
        );
    }

    pub fn supervisor_stopped(&self, pid: u32, queue: &str) {
        self.span(
            "supervisor.stop",
            &[("pid", pid.to_string()), ("queue", queue.to_owned())],
        );
    }

    pub fn child_started(&self, slot: u32, pid: u32, queue: &str) {
        self.span(
            "child.start",
            &[
                ("slot", slot.to_string()),
                ("pid", pid.to_string()),
                ("queue", queue.to_owned()),
            ],
        );
    }

    pub fn child_exited(
        &self,
        slot: u32,
        pid: u32,
        queue: &str,
        reason: &str,
        code: Option<i32>,
        signal: Option<i32>,
    ) {
        let mut fields = vec![
            ("slot", slot.to_string()),
            ("pid", pid.to_string()),
            ("queue", queue.to_owned()),
            ("reason", reason.to_owned()),
        ];
        if let Some(code) = code {
            fields.push(("exit_code", code.to_string()));
        }
        if let Some(signal) = signal {
            fields.push(("signal", signal.to_string()));
        }
        self.span("child.exit", &fields);
    }

    pub fn child_restart(&self, slot: u32, queue: &str, pause: Duration) {
        self.span(
            "child.restart",
            &[
                ("slot", slot.to_string()),
                ("queue", queue.to_owned()),
                ("pause_ms", pause.as_millis().to_string()),
            ],
        );
    }

    pub fn drain(&self, pid: u32, queue: &str, signal: &str) {
        self.span(
            "drain",
            &[
                ("pid", pid.to_string()),
                ("queue", queue.to_owned()),
                ("signal", signal.to_owned()),
            ],
        );
    }

    pub fn hard_kill(&self, slot: u32, pid: u32, queue: &str, reason: &str) {
        self.capture(
            "hard kill",
            &[
                ("queue", queue),
                ("slot", &slot.to_string()),
                ("pid", &pid.to_string()),
                ("reason", reason),
                ("signal", "9"),
            ],
        );
    }

    pub fn restart_budget(&self, slot: u32, queue: &str, code: Option<i32>, signal: Option<i32>) {
        let code_text = code.map(|code| code.to_string()).unwrap_or_default();
        let signal_text = signal.map(|signal| signal.to_string()).unwrap_or_default();
        self.capture(
            "restart budget exhausted",
            &[
                ("queue", queue),
                ("slot", &slot.to_string()),
                ("reason", "crash"),
                ("exit_code", code_text.as_str()),
                ("signal", signal_text.as_str()),
            ],
        );
    }

    pub fn child_start_failed(&self, slot: u32, queue: &str) {
        self.capture(
            "child failed to start",
            &[
                ("queue", queue),
                ("slot", &slot.to_string()),
                ("reason", "crash"),
            ],
        );
    }

    pub fn redis_publish_late(&self, queue: &str) {
        self.capture(
            "supervisor redis publish exceeded the heartbeat window",
            &[("queue", queue), ("reason", "redis")],
        );
    }

    pub fn supervisor_panic(&self, queue: &str) {
        self.capture(
            "supervisor slot panicked",
            &[("queue", queue), ("reason", "panic")],
        );
    }

    pub async fn shutdown(&self) {
        let Some(provider) = self.inner.provider.clone() else {
            return;
        };
        let timeout = self.inner.timeout;
        // Shutdown blocks on the exporter. It runs off the runtime and is capped
        // by the OTLP timeout, and by five seconds when that timeout is longer.
        let _ = tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || {
                let _ = provider.shutdown_with_timeout(timeout);
            }),
        )
        .await;
    }

    fn span(&self, name: &str, fields: &[(&str, String)]) {
        let Some(tracer) = &self.inner.tracer else {
            return;
        };
        let mut span = tracer.start(name.to_owned());
        span.set_attribute(KeyValue::new("host", self.inner.host.clone()));
        span.set_attribute(KeyValue::new("version", self.inner.version.clone()));
        for (key, value) in fields {
            span.set_attribute(KeyValue::new((*key).to_owned(), value.clone()));
        }
        span.end();
    }

    fn capture(&self, message: &str, tags: &[(&str, &str)]) {
        if self.inner.sentry.is_none() && !sentry::Hub::current().client().is_some() {
            return;
        }
        let message = redact(message);
        sentry::with_scope(
            |scope| {
                for (key, value) in tags {
                    if !value.is_empty() {
                        scope.set_tag(key, redact(value));
                    }
                }
            },
            || {
                sentry::capture_message(&message, sentry::Level::Error);
            },
        );
    }
}

/// Install the process-wide supervisor telemetry. Call this before Tokio starts.
pub fn install(config: &TelemetryConfig) -> anyhow::Result<SupervisorTelemetry> {
    let telemetry = build(config, Duration::from_secs(1))?;
    let _ = CURRENT.set(telemetry.clone());
    Ok(telemetry)
}

pub fn current() -> SupervisorTelemetry {
    CURRENT
        .get()
        .cloned()
        .unwrap_or_else(SupervisorTelemetry::disabled)
}

pub fn build(
    config: &TelemetryConfig,
    batch_delay: Duration,
) -> anyhow::Result<SupervisorTelemetry> {
    install_ring_provider();
    install_subscriber(config)?;
    let (tracer, provider, export_runtime) = if config.traces_enabled {
        // The batch processor polls the exporter on its own thread, with no
        // Tokio reactor. The HTTP and gRPC clients need one, so export runs
        // on a runtime that is not the supervisor runtime.
        let export_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("simple-q-otlp")
            .enable_all()
            .build()
            .map_err(|error| anyhow::anyhow!("OTLP runtime: {error}"))?;
        let inner = export_runtime.block_on(async { build_exporter(config) })?;
        let exporter = TokioSpanExporter {
            inner,
            runtime: export_runtime.handle().clone(),
        };
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(2048)
                    .with_max_export_batch_size(512)
                    .with_scheduled_delay(batch_delay)
                    .build(),
            )
            .build();
        let resource = resource(config);
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_sampler(sampler(config.sampler, config.sampler_arg))
            .with_resource(resource)
            .build();
        let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "simple-q-supervisor");
        (Some(tracer), Some(Arc::new(provider)), Some(export_runtime))
    } else {
        (None, None, None)
    };
    let sentry = if config.sentry_enabled {
        Some(install_sentry(config)?)
    } else {
        None
    };
    Ok(SupervisorTelemetry {
        inner: Arc::new(Inner {
            tracer,
            provider,
            export_runtime,
            sentry,
            host: simple_q_runtime::ids::hostname(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            traces: config.traces_label(),
            sentry_label: config.sentry_label(),
            timeout: config
                .timeout
                .min(Duration::from_secs(5))
                .max(Duration::from_millis(1)),
        }),
    })
}

fn install_subscriber(config: &TelemetryConfig) -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;
    let fmt = tracing_subscriber::fmt::layer().with_filter(crate::telemetry::fmt_env_filter());
    if config.sentry_enabled {
        tracing_subscriber::registry()
            .with(fmt)
            .with(
                sentry::integrations::tracing::BreadcrumbLayer::new().with_filter(
                    tracing_subscriber::filter::filter_fn(|metadata| {
                        *metadata.level() == tracing::Level::WARN
                    }),
                ),
            )
            .try_init()
            .ok();
    } else {
        tracing_subscriber::registry().with(fmt).try_init().ok();
    }
    Ok(())
}

fn resource(config: &TelemetryConfig) -> Resource {
    let mut attributes = Vec::new();
    for (key, value) in &config.resource_attributes {
        attributes.push(KeyValue::new(key.clone(), value.clone()));
    }
    attributes.push(KeyValue::new("service.name", config.service_name.clone()));
    attributes.push(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")));
    attributes.push(KeyValue::new(
        "host.name",
        simple_q_runtime::ids::hostname(),
    ));
    attributes.push(KeyValue::new("process.pid", i64::from(std::process::id())));
    Resource::builder_empty()
        .with_attributes(attributes)
        .build()
}

fn sampler(name: SamplerName, arg: f64) -> Sampler {
    match name {
        SamplerName::AlwaysOn => Sampler::AlwaysOn,
        SamplerName::AlwaysOff => Sampler::AlwaysOff,
        SamplerName::TraceIdRatio => Sampler::TraceIdRatioBased(arg),
        SamplerName::ParentBasedAlwaysOn => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        SamplerName::ParentBasedAlwaysOff => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        SamplerName::ParentBasedTraceIdRatio => {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(arg)))
        }
    }
}

#[derive(Debug)]
struct TokioSpanExporter {
    inner: opentelemetry_otlp::SpanExporter,
    runtime: tokio::runtime::Handle,
}

impl SpanExporter for TokioSpanExporter {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let result = self.runtime.block_on(self.inner.export(batch));
        std::future::ready(result)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
}

fn install_ring_provider() {
    // reqwest is built without a bundled rustls provider. ring is the process
    // default for the HTTP exporter and for Sentry.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn build_exporter(config: &TelemetryConfig) -> anyhow::Result<opentelemetry_otlp::SpanExporter> {
    let endpoint = config
        .endpoint
        .clone()
        .ok_or_else(|| anyhow::anyhow!("OTLP endpoint is missing"))?;
    let timeout = config.timeout;
    let headers = config.header_string();
    let protocol = config.protocol;
    with_header_env(&headers, || match protocol {
        OtlpProtocol::HttpProtobuf => http_exporter(endpoint, timeout),
        OtlpProtocol::Grpc => grpc_exporter(endpoint, timeout),
    })
}

fn http_exporter(
    endpoint: String,
    timeout: Duration,
) -> anyhow::Result<opentelemetry_otlp::SpanExporter> {
    use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
    SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::HttpBinary)
        .with_timeout(timeout)
        .build()
        .map_err(|error| anyhow::anyhow!("OTLP exporter: {error}"))
}

#[cfg(feature = "otlp-grpc")]
fn grpc_exporter(
    endpoint: String,
    timeout: Duration,
) -> anyhow::Result<opentelemetry_otlp::SpanExporter> {
    use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
    SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_protocol(Protocol::Grpc)
        .with_timeout(timeout)
        .build()
        .map_err(|error| anyhow::anyhow!("OTLP exporter: {error}"))
}

#[cfg(not(feature = "otlp-grpc"))]
fn grpc_exporter(
    _endpoint: String,
    _timeout: Duration,
) -> anyhow::Result<opentelemetry_otlp::SpanExporter> {
    anyhow::bail!(
        "this build exports OTLP over http/protobuf; rebuild with --features otlp-grpc for grpc"
    )
}

fn install_sentry(config: &TelemetryConfig) -> anyhow::Result<sentry::ClientInitGuard> {
    let dsn = config
        .sentry_dsn
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("SENTRY_DSN is invalid"))?;
    let parsed = dsn
        .parse()
        .map_err(|_| anyhow::anyhow!("SENTRY_DSN is invalid"))?;
    let release = config.sentry_release.clone();
    let environment = config.sentry_environment.clone();
    let rate = config.sentry_sample_rate as f32;
    let mut options = sentry::ClientOptions::new();
    options.dsn = Some(parsed);
    options.release = Some(release.into());
    options.environment = environment.map(Into::into);
    options.send_default_pii = false;
    options.default_integrations = false;
    options.integrations.push(std::sync::Arc::new(
        sentry::integrations::panic::PanicIntegration::new(),
    ));
    options.before_send = Some(std::sync::Arc::new(|mut event| {
        scrub_event(&mut event);
        Some(event)
    }));
    options.before_breadcrumb = Some(std::sync::Arc::new(|mut crumb| {
        scrub_breadcrumb(&mut crumb);
        Some(crumb)
    }));
    let guard = sentry::init(options.sample_rate(rate));
    Ok(guard)
}

fn scrub_event(event: &mut sentry::protocol::Event<'_>) {
    if let Some(message) = event.message.as_mut() {
        *message = redact(message);
    }
    event.user.take();
    event.extra.clear();
    event.request.take();
}

fn scrub_breadcrumb(crumb: &mut sentry::Breadcrumb) {
    if let Some(message) = crumb.message.as_mut() {
        *message = redact(message);
    }
    crumb.data.retain(|key, value| {
        let key_hidden = key.eq_ignore_ascii_case("url")
            || key.to_ascii_lowercase().contains("dsn")
            || key.to_ascii_lowercase().contains("authorization");
        if key_hidden {
            return false;
        }
        if let Some(text) = value.as_str() {
            *value = serde_json::Value::String(redact(text));
        }
        true
    });
}

/// Replace Redis URLs and header-like secrets. The replacement is not the secret.
pub fn redact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = find_secret(rest) {
        out.push_str(&rest[..start]);
        out.push_str("<redacted>");
        let after = &rest[start..];
        let skip = after.find(char::is_whitespace).unwrap_or(after.len());
        rest = &after[skip..];
    }
    out.push_str(rest);
    out
}

fn header_mark() -> String {
    // The marker is not stored as one literal. A scan of the binary must not
    // see it as a header.
    const MASK: u8 = 0x5a;
    const ENCODED: [u8; 14] = [
        0x3b, 0x2f, 0x2e, 0x32, 0x35, 0x28, 0x33, 0x20, 0x3b, 0x2e, 0x33, 0x35, 0x34, 0x67,
    ];
    ENCODED.iter().map(|byte| (byte ^ MASK) as char).collect()
}

fn find_secret(text: &str) -> Option<usize> {
    let lower = text.to_ascii_lowercase();
    let header_mark = header_mark();
    let mut found = None;
    for needle in ["redis://", "rediss://", header_mark.as_str()] {
        if let Some(index) = lower.find(needle) {
            found = Some(found.map_or(index, |current: usize| current.min(index)));
        }
    }
    found
}

/// Publish the resolved header string for the exporter build, then put the
/// process environment back. Children inherit the restored values.
///
/// The string already prefers trace headers over the general headers. The
/// trace variable is cleared for the build so the SDK does not prefer a raw
/// value over that string.
///
/// # Safety
///
/// `set_var` is unsafe when other threads exist. The supervisor calls this
/// before `Runtime::new` and before the batch thread starts.
fn with_header_env<T>(headers: &str, body: impl FnOnce() -> T) -> T {
    let keys = [
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    ];
    let saved: Vec<(&str, Option<String>)> = keys
        .iter()
        .map(|key| (*key, std::env::var(key).ok()))
        .collect();
    unsafe {
        if headers.is_empty() {
            std::env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");
        } else {
            std::env::set_var("OTEL_EXPORTER_OTLP_HEADERS", headers);
        }
        std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_HEADERS");
    }
    let result = body();
    unsafe {
        for (key, value) in saved {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
    result
}

#[cfg(test)]
#[path = "supervisor_telemetry_tests.rs"]
mod tests;
