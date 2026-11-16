use super::*;
use opentelemetry::trace::TraceContextExt;
use tracing::Level;
use tracing_subscriber::Layer;

#[test]
fn install_without_tracing_does_not_export() {
    // Arrange

    let before = python_export_calls();
    // Act

    install(None).unwrap();
    // Assert

    assert_eq!(python_export_calls(), before);
}

#[test]
fn otel_targets_stay_independent_of_the_fmt_filter() {
    // Arrange

    let fmt = EnvFilter::new("warn");
    // Act

    let targets = otel_targets(&[
        ("agent_worker".into(), "info".into()),
        ("simple_q_runtime".into(), "info".into()),
        ("apalis".into(), "debug".into()),
    ])
    .unwrap();
    // Assert

    assert!(targets.would_enable("agent_worker", &Level::INFO));
    assert!(targets.would_enable("agent_worker::worker", &Level::INFO));
    assert!(targets.would_enable("simple_q_runtime", &Level::INFO));
    assert!(targets.would_enable("apalis", &Level::DEBUG));
    assert!(!targets.would_enable("other_target", &Level::INFO));
    let _ = fmt;
}

#[test]
fn warn_fmt_filter_still_exports_an_info_span() {
    // Arrange

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
    // Act

    let recorded = names.lock().expect("names").clone();
    // Assert

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
    // Arrange

    let mut headers = BTreeMap::new();
    headers.insert(
        "traceparent".into(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03".into(),
    );
    headers.insert("tracestate".into(), "vendor=opaque-value,other=123".into());
    let context = parent_context(&headers).expect("traceparent");
    // Act

    let span_context = context.span().span_context().clone();
    // Assert

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
    // Arrange

    for value in [
        "invalid",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
    ] {
        let mut headers = BTreeMap::new();
        headers.insert("traceparent".into(), value.into());
        assert!(parent_context(&headers).is_none(), "{value}");
    }
    // Act

    // Assert

    assert!(parent_context(&BTreeMap::new()).is_none());
}

#[test]
fn parser_rejects_exporter_resource_and_targets() {
    // Arrange

    Python::initialize(); // Act

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
        let bad_exporter: Py<PyAny> = locals.get_item("bad_exporter").unwrap().unwrap().unbind();
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
    }); // Assert
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
