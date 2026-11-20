use super::*;

#[cfg(not(feature = "otlp-grpc"))]
#[test]
fn given_grpc_on_the_default_build_when_the_exporter_starts_then_http_is_required() {
    // Arrange
    let config = config_from(
        simple_q_parent::TelemetryFlags {
            otel_endpoint: Some("http://127.0.0.1:9".to_owned()),
            otel_protocol: Some("grpc".to_owned()),
            ..simple_q_parent::TelemetryFlags::default()
        },
        &[],
    );

    // Act
    let error = build_exporter(&config).unwrap_err();

    // Assert
    let text = error.to_string();
    assert!(text.contains("http/protobuf"));
    assert!(text.contains("otlp-grpc"));
}

#[test]
fn given_no_settings_when_built_then_there_is_no_exporter() {
    // Arrange
    let config = simple_q_parent::resolve_telemetry(
        &simple_q_parent::TelemetryFlags::default(),
        |_| None,
        "2.0.0-alpha.1",
    )
    .unwrap();

    // Act
    let telemetry = build(&config, Duration::from_millis(50)).unwrap();

    // Assert
    assert!(telemetry.inner.provider.is_none());
    assert!(telemetry.inner.sentry.is_none());
    assert_eq!(telemetry.traces_label(), "off");
    assert_eq!(telemetry.sentry_label(), "off");
}

#[test]
fn given_redis_url_when_redacted_then_the_secret_is_gone() {
    // Arrange
    let text = "connect redis://:hidden@redis.internal:6379/15 failed";

    // Act
    let redacted = redact(text);

    // Assert
    assert!(!redacted.contains("hidden"));
    assert!(!redacted.contains("redis://"));
    assert!(redacted.contains("<redacted>"));
    let header = format!("{}={}", "Authorization", "top-secret-header");
    let redacted = redact(&format!("{header} next"));
    assert!(!redacted.contains("top-secret-header"));
    assert!(redacted.contains("<redacted>"));
}

fn config_from(
    flags: simple_q_parent::TelemetryFlags,
    values: &[(&str, &str)],
) -> simple_q_parent::TelemetryConfig {
    simple_q_parent::resolve_telemetry(
        &flags,
        |key| {
            values
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        },
        "2.0.0-alpha.1",
    )
    .unwrap()
}

#[test]
fn a_local_collector_receives_start_exit_and_restart_spans() {
    // Arrange
    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let listener = server_runtime
        .block_on(async { tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let port = listener.local_addr().unwrap().port();
    let body = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let stored = body.clone();
    let server = server_runtime.spawn(async move {
        let Ok(accepted) = tokio::time::timeout(Duration::from_secs(8), listener.accept()).await
        else {
            return;
        };
        let (mut socket, _) = accepted.unwrap();
        let payload = read_otlp(&mut socket).await;
        *stored.lock().unwrap() = payload;
    });
    let config = config_from(
        simple_q_parent::TelemetryFlags {
            otel_endpoint: Some(format!("http://127.0.0.1:{port}")),
            otel_protocol: Some("http/protobuf".to_owned()),
            ..simple_q_parent::TelemetryFlags::default()
        },
        &[("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")],
    );
    let telemetry = build(&config, Duration::from_millis(10)).unwrap();

    // Act
    telemetry.supervisor_started(1, "reports");
    telemetry.child_exited(0, 2, "reports", "crash", Some(1), None);
    telemetry.child_restart(0, "reports", Duration::from_secs(1));
    server_runtime.block_on(telemetry.shutdown());
    server_runtime.block_on(async {
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
    });
    drop(telemetry);

    // Assert
    let payload = body.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&payload);
    assert!(text.contains("supervisor.start"), "{text}");
    assert!(text.contains("child.exit"), "{text}");
    assert!(text.contains("child.restart"), "{text}");
}

#[tokio::test]
async fn given_no_endpoint_when_spans_are_emitted_then_nothing_connects() {
    // Arrange
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = config_from(
        simple_q_parent::TelemetryFlags {
            otel_endpoint: Some(format!("http://127.0.0.1:{port}")),
            ..simple_q_parent::TelemetryFlags::default()
        },
        &[("OTEL_SDK_DISABLED", "true")],
    );
    assert!(!config.traces_enabled);
    let telemetry = build(&config, Duration::from_millis(10)).unwrap();

    // Act
    telemetry.supervisor_started(1, "reports");
    telemetry.child_exited(0, 2, "reports", "crash", Some(1), None);
    telemetry.child_restart(0, "reports", Duration::from_secs(1));
    telemetry.shutdown().await;
    let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;

    // Assert
    assert!(accepted.is_err());
}

#[test]
fn a_test_transport_records_the_crash_loop_and_the_hard_kill() {
    // Arrange
    let telemetry = SupervisorTelemetry::disabled();

    // Act
    let events = sentry::test::with_captured_events(|| {
        telemetry.restart_budget(1, "reports", Some(1), None);
        telemetry.hard_kill(2, 99, "redis://:hidden@redis:6379/0", "hard kill");
    });

    // Assert
    let rendered = format!("{events:?}");
    assert!(rendered.contains("restart budget exhausted"));
    assert!(rendered.contains("hard kill"));
    assert!(!rendered.contains("hidden"));
    assert!(!rendered.contains("redis://"));
}

#[test]
fn dsn_and_headers_stay_out_of_debug_logs_and_the_record_fields() {
    // Arrange
    let dsn = "https://public@sentry.example/1";
    let header = "top-secret-header";
    let config = config_from(
        simple_q_parent::TelemetryFlags::default(),
        &[
            ("SENTRY_DSN", dsn),
            (
                "OTEL_EXPORTER_OTLP_HEADERS",
                &format!("{}={}", "Authorization", header),
            ),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318"),
        ],
    );

    // Act
    let debug = format!("{config:?}");
    let record = format!(
        "redis_host=redis redis_db=0 traces={} sentry={}",
        config.traces_label(),
        config.sentry_label()
    );
    let info = serde_json::json!({
        "traces": config.traces_label(),
        "sentry": config.sentry_label(),
        "redis_host": "redis",
        "redis_db": 0,
    });
    let info_json = info.to_string();
    let log_line = format!("supervisor telemetry {debug} record {record}");

    // Assert
    assert!(!debug.contains(header));
    assert!(!debug.contains("public@"));
    assert!(!debug.contains(dsn));
    assert!(!record.contains(header));
    assert!(!record.contains(dsn));
    assert!(!info_json.contains(header));
    assert!(!info_json.contains(dsn));
    assert!(!log_line.contains(header));
    assert!(!log_line.contains("public@"));
    assert_eq!(config.traces_label(), "on");
    assert_eq!(config.sentry_label(), "on");
}

async fn read_otlp(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut total = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await;
        let count = match read {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(count)) => count,
            Ok(Err(_)) => break,
        };
        total.extend_from_slice(&buf[..count]);
        if let Some(header_end) = total.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&total[..header_end]).to_ascii_lowercase();
            let length = headers.lines().find_map(|line| {
                line.strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            });
            let body_at = header_end + 4;
            if let Some(length) = length
                && total.len() >= body_at + length
            {
                let body = total[body_at..body_at + length].to_vec();
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
                return body;
            }
        }
        if total.len() > 1_000_000 {
            break;
        }
    }
    total
}
