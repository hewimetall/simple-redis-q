//! Supervisor telemetry settings.
//!
//! Priority is the CLI flag, then `SIMPLE_Q_SUPERVISOR_<NAME>`, then the
//! standard variable, then the default. Secrets are never taken from flags.
//! `Debug` omits the DSN and header values.

use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpProtocol {
    HttpProtobuf,
    Grpc,
}

impl OtlpProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HttpProtobuf => "http/protobuf",
            Self::Grpc => "grpc",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplerName {
    AlwaysOn,
    AlwaysOff,
    TraceIdRatio,
    ParentBasedAlwaysOn,
    ParentBasedAlwaysOff,
    ParentBasedTraceIdRatio,
}

impl SamplerName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlwaysOn => "always_on",
            Self::AlwaysOff => "always_off",
            Self::TraceIdRatio => "traceidratio",
            Self::ParentBasedAlwaysOn => "parentbased_always_on",
            Self::ParentBasedAlwaysOff => "parentbased_always_off",
            Self::ParentBasedTraceIdRatio => "parentbased_traceidratio",
        }
    }
}

/// Flags the parent may set. Secrets are absent on purpose.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TelemetryFlags {
    pub otel_endpoint: Option<String>,
    pub otel_protocol: Option<String>,
    pub otel_service_name: Option<String>,
    pub otel_sampler: Option<String>,
    pub sentry_environment: Option<String>,
    pub sentry_release: Option<String>,
    pub sentry_sample_rate: Option<String>,
}

/// Resolved supervisor telemetry. Header values and the DSN are not in `Debug`.
#[derive(Clone, PartialEq)]
pub struct TelemetryConfig {
    pub traces_enabled: bool,
    pub sentry_enabled: bool,
    pub endpoint: Option<String>,
    pub protocol: OtlpProtocol,
    pub service_name: String,
    pub resource_attributes: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub timeout: Duration,
    pub sampler: SamplerName,
    pub sampler_arg: f64,
    pub sentry_dsn: Option<String>,
    pub sentry_environment: Option<String>,
    pub sentry_release: String,
    pub sentry_sample_rate: f64,
}

impl std::fmt::Debug for TelemetryConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetryConfig")
            .field("traces_enabled", &self.traces_enabled)
            .field("sentry_enabled", &self.sentry_enabled)
            .field("endpoint", &redact_endpoint(self.endpoint.as_deref()))
            .field("protocol", &self.protocol)
            .field("service_name", &self.service_name)
            .field(
                "resource_attributes",
                &self.resource_attributes.keys().collect::<Vec<_>>(),
            )
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("timeout", &self.timeout)
            .field("sampler", &self.sampler)
            .field("sampler_arg", &self.sampler_arg)
            .field("sentry_dsn", &self.sentry_dsn.as_ref().map(|_| "set"))
            .field("sentry_environment", &self.sentry_environment)
            .field("sentry_release", &self.sentry_release)
            .field("sentry_sample_rate", &self.sentry_sample_rate)
            .finish()
    }
}

impl TelemetryConfig {
    pub fn traces_label(&self) -> &'static str {
        if self.traces_enabled { "on" } else { "off" }
    }

    pub fn sentry_label(&self) -> &'static str {
        if self.sentry_enabled { "on" } else { "off" }
    }

    /// `key=value,key2=value2` for the exporter. Empty when there are no headers.
    pub fn header_string(&self) -> String {
        self.headers
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryError {
    message: String,
}

impl TelemetryError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TelemetryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TelemetryError {}

const SERVICE_NAME: &str = "simple-q-supervisor";

/// Resolve supervisor telemetry.
///
/// `getenv` is called for both the `SIMPLE_Q_SUPERVISOR_` name and the
/// standard name. The release default is the simple-q version.
pub fn resolve_telemetry(
    flags: &TelemetryFlags,
    mut getenv: impl FnMut(&str) -> Option<String>,
    release_default: &str,
) -> Result<TelemetryConfig, TelemetryError> {
    let sdk_disabled = read_bool(
        "OTEL_SDK_DISABLED",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_SDK_DISABLED"),
            getenv("OTEL_SDK_DISABLED"),
        ),
        false,
    )?;
    let service_name = pick(
        &flags.otel_service_name,
        getenv("SIMPLE_Q_SUPERVISOR_OTEL_SERVICE_NAME"),
        getenv("OTEL_SERVICE_NAME"),
    )
    .unwrap_or_else(|| SERVICE_NAME.to_owned());
    let resource_attributes = parse_attributes(
        "OTEL_RESOURCE_ATTRIBUTES",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_RESOURCE_ATTRIBUTES"),
            getenv("OTEL_RESOURCE_ATTRIBUTES"),
        ),
    )?;
    let endpoint = optional_url(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        pick(
            &flags.otel_endpoint,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_EXPORTER_OTLP_ENDPOINT"),
            getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
        ),
    )?;
    let traces_endpoint = optional_url(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
        ),
    )?;
    let effective = traces_endpoint.or(endpoint);
    let protocol = parse_protocol(
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        pick(
            &flags.otel_protocol,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_EXPORTER_OTLP_PROTOCOL"),
            getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
        ),
    )?;
    let headers = parse_headers(
        "OTEL_EXPORTER_OTLP_HEADERS",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_EXPORTER_OTLP_HEADERS"),
            getenv("OTEL_EXPORTER_OTLP_HEADERS"),
        ),
    )?;
    let timeout = parse_timeout(
        "OTEL_EXPORTER_OTLP_TIMEOUT",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_EXPORTER_OTLP_TIMEOUT"),
            getenv("OTEL_EXPORTER_OTLP_TIMEOUT"),
        ),
    )?;
    let sampler = parse_sampler(
        "OTEL_TRACES_SAMPLER",
        pick(
            &flags.otel_sampler,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_TRACES_SAMPLER"),
            getenv("OTEL_TRACES_SAMPLER"),
        ),
    )?;
    let sampler_arg = parse_unit(
        "OTEL_TRACES_SAMPLER_ARG",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_OTEL_TRACES_SAMPLER_ARG"),
            getenv("OTEL_TRACES_SAMPLER_ARG"),
        ),
        1.0,
    )?;
    let dsn = secret(
        "SENTRY_DSN",
        pick(
            &None,
            getenv("SIMPLE_Q_SUPERVISOR_SENTRY_DSN"),
            getenv("SENTRY_DSN"),
        ),
    )?;
    let environment = blank_to_none(pick(
        &flags.sentry_environment,
        getenv("SIMPLE_Q_SUPERVISOR_SENTRY_ENVIRONMENT"),
        getenv("SENTRY_ENVIRONMENT"),
    ));
    let release = pick(
        &flags.sentry_release,
        getenv("SIMPLE_Q_SUPERVISOR_SENTRY_RELEASE"),
        getenv("SENTRY_RELEASE"),
    )
    .unwrap_or_else(|| release_default.to_owned());
    let sample_rate = parse_unit(
        "SENTRY_SAMPLE_RATE",
        pick(
            &flags.sentry_sample_rate,
            getenv("SIMPLE_Q_SUPERVISOR_SENTRY_SAMPLE_RATE"),
            getenv("SENTRY_SAMPLE_RATE"),
        ),
        1.0,
    )?;
    if release.is_empty() {
        return Err(TelemetryError::new("SENTRY_RELEASE is empty"));
    }
    Ok(TelemetryConfig {
        traces_enabled: !sdk_disabled && effective.is_some(),
        sentry_enabled: dsn.is_some(),
        endpoint: effective,
        protocol,
        service_name,
        resource_attributes,
        headers,
        timeout,
        sampler,
        sampler_arg,
        sentry_dsn: dsn,
        sentry_environment: environment,
        sentry_release: release,
        sentry_sample_rate: sample_rate,
    })
}

fn pick(
    flag: &Option<String>,
    supervisor: Option<String>,
    standard: Option<String>,
) -> Option<String> {
    nonempty(flag.clone())
        .or(nonempty(supervisor))
        .or(nonempty(standard))
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.is_empty())
}

fn blank_to_none(value: Option<String>) -> Option<String> {
    nonempty(value)
}

fn read_bool(name: &str, value: Option<String>, default: bool) -> Result<bool, TelemetryError> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(TelemetryError::new(format!("{name} is invalid"))),
    }
}

fn optional_url(name: &str, value: Option<String>) -> Result<Option<String>, TelemetryError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(TelemetryError::new(format!(
            "{name} must be an http or https URL"
        )));
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err(TelemetryError::new(format!("{name} is invalid")));
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_protocol(name: &str, value: Option<String>) -> Result<OtlpProtocol, TelemetryError> {
    let Some(value) = value else {
        return Ok(OtlpProtocol::HttpProtobuf);
    };
    match value.trim() {
        "http/protobuf" => Ok(OtlpProtocol::HttpProtobuf),
        "grpc" => Ok(OtlpProtocol::Grpc),
        _ => Err(TelemetryError::new(format!(
            "{name} must be http/protobuf or grpc"
        ))),
    }
}

fn parse_sampler(name: &str, value: Option<String>) -> Result<SamplerName, TelemetryError> {
    let Some(value) = value else {
        return Ok(SamplerName::ParentBasedAlwaysOn);
    };
    match value.trim() {
        "always_on" => Ok(SamplerName::AlwaysOn),
        "always_off" => Ok(SamplerName::AlwaysOff),
        "traceidratio" => Ok(SamplerName::TraceIdRatio),
        "parentbased_always_on" => Ok(SamplerName::ParentBasedAlwaysOn),
        "parentbased_always_off" => Ok(SamplerName::ParentBasedAlwaysOff),
        "parentbased_traceidratio" => Ok(SamplerName::ParentBasedTraceIdRatio),
        _ => Err(TelemetryError::new(format!("{name} is invalid"))),
    }
}

fn parse_unit(name: &str, value: Option<String>, default: f64) -> Result<f64, TelemetryError> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed: f64 = value
        .trim()
        .parse()
        .map_err(|_| TelemetryError::new(format!("{name} is invalid")))?;
    if !parsed.is_finite() || !(0.0..=1.0).contains(&parsed) {
        return Err(TelemetryError::new(format!(
            "{name} must be between 0 and 1"
        )));
    }
    Ok(parsed)
}

fn parse_timeout(name: &str, value: Option<String>) -> Result<Duration, TelemetryError> {
    let Some(value) = value else {
        return Ok(Duration::from_millis(10_000));
    };
    let trimmed = value.trim();
    let (number, unit) = if let Some(number) = trimmed.strip_suffix("ms") {
        (number, "ms")
    } else if let Some(number) = trimmed.strip_suffix('s') {
        (number, "s")
    } else {
        (trimmed, "ms")
    };
    let number: u64 = number
        .trim()
        .parse()
        .map_err(|_| TelemetryError::new(format!("{name} is invalid")))?;
    if number == 0 {
        return Err(TelemetryError::new(format!("{name} must be positive")));
    }
    let duration = if unit == "s" {
        Duration::from_secs(number)
    } else {
        Duration::from_millis(number)
    };
    Ok(duration)
}

fn parse_attributes(
    name: &str,
    value: Option<String>,
) -> Result<BTreeMap<String, String>, TelemetryError> {
    parse_pairs(name, value)
}

fn parse_headers(
    name: &str,
    value: Option<String>,
) -> Result<BTreeMap<String, String>, TelemetryError> {
    parse_pairs(name, value)
}

fn parse_pairs(
    name: &str,
    value: Option<String>,
) -> Result<BTreeMap<String, String>, TelemetryError> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let mut pairs = BTreeMap::new();
    if value.trim().is_empty() {
        return Ok(pairs);
    }
    for segment in value.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            return Err(TelemetryError::new(format!(
                "{name} has an invalid segment"
            )));
        }
        let Some((key, raw)) = segment.split_once('=') else {
            return Err(TelemetryError::new(format!(
                "{name} has an invalid segment"
            )));
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(TelemetryError::new(format!(
                "{name} has an invalid segment"
            )));
        }
        pairs.insert(key.to_owned(), raw.trim().to_owned());
    }
    Ok(pairs)
}

fn secret(name: &str, value: Option<String>) -> Result<Option<String>, TelemetryError> {
    let Some(value) = nonempty(value) else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if name == "SENTRY_DSN" && !trimmed.contains("://") {
        return Err(TelemetryError::new("SENTRY_DSN is invalid"));
    }
    Ok(Some(trimmed.to_owned()))
}

fn redact_endpoint(endpoint: Option<&str>) -> Option<String> {
    let endpoint = endpoint?;
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return Some("<redacted>".to_owned());
    };
    let host = rest.split_once('@').map(|(_, host)| host).unwrap_or(rest);
    Some(format!("{scheme}://{host}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn given_no_settings_when_resolved_then_export_is_off() {
        // Arrange
        let flags = TelemetryFlags::default();

        // Act
        let config = resolve_telemetry(&flags, env(&[]), "2.0.0-alpha.1").unwrap();

        // Assert
        assert!(!config.traces_enabled);
        assert!(!config.sentry_enabled);
        assert!(config.endpoint.is_none());
        assert!(config.sentry_dsn.is_none());
        assert_eq!(config.service_name, "simple-q-supervisor");
        assert_eq!(config.sentry_release, "2.0.0-alpha.1");
        assert_eq!(config.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(config.traces_label(), "off");
        assert_eq!(config.sentry_label(), "off");
    }

    #[test]
    fn given_supervisor_var_and_standard_var_when_resolved_then_supervisor_wins() {
        // Arrange
        let flags = TelemetryFlags::default();
        let getenv = env(&[
            (
                "SIMPLE_Q_SUPERVISOR_OTEL_SERVICE_NAME",
                "reports-supervisor",
            ),
            ("OTEL_SERVICE_NAME", "other"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318"),
        ]);

        // Act
        let config = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap();

        // Assert
        assert_eq!(config.service_name, "reports-supervisor");
        assert!(config.traces_enabled);
    }

    #[test]
    fn given_flag_when_resolved_then_flag_wins_over_both_variables() {
        // Arrange
        let flags = TelemetryFlags {
            otel_service_name: Some("from-flag".to_owned()),
            ..TelemetryFlags::default()
        };
        let getenv = env(&[
            ("SIMPLE_Q_SUPERVISOR_OTEL_SERVICE_NAME", "from-supervisor"),
            ("OTEL_SERVICE_NAME", "from-standard"),
        ]);

        // Act
        let config = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap();

        // Assert
        assert_eq!(config.service_name, "from-flag");
    }

    #[test]
    fn given_traces_endpoint_when_resolved_then_it_replaces_the_general_endpoint() {
        // Arrange
        let flags = TelemetryFlags {
            otel_endpoint: Some("http://otel-collector:4318".to_owned()),
            ..TelemetryFlags::default()
        };
        let getenv = env(&[(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://otel-collector:4318/v1/traces",
        )]);

        // Act
        let config = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap();

        // Assert
        assert_eq!(
            config.endpoint.as_deref(),
            Some("http://otel-collector:4318/v1/traces")
        );
    }

    #[test]
    fn given_sdk_disabled_when_endpoint_set_then_traces_stay_off() {
        // Arrange
        let flags = TelemetryFlags::default();
        let getenv = env(&[
            ("OTEL_SDK_DISABLED", "true"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318"),
        ]);

        // Act
        let config = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap();

        // Assert
        assert!(!config.traces_enabled);
        assert!(config.endpoint.is_some());
    }

    #[test]
    fn given_bad_protocol_when_resolved_then_error_has_no_secret() {
        // Arrange
        let flags = TelemetryFlags::default();
        let getenv = env(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")]);

        // Act
        let error = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap_err();

        // Assert
        assert!(error.to_string().contains("http/protobuf"));
        assert!(!error.to_string().contains("http/json"));
    }

    #[test]
    fn given_dsn_and_headers_when_debug_then_values_are_absent() {
        // Arrange
        let dsn = "https://public@sentry.example/1";
        let token = "top-secret-header";
        let header = format!("{}={}", "Authorization", token);
        let flags = TelemetryFlags::default();
        let getenv = env(&[
            ("SENTRY_DSN", dsn),
            ("OTEL_EXPORTER_OTLP_HEADERS", header.as_str()),
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "https://user:hidden@otel-collector:4318",
            ),
        ]);

        // Act
        let config = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap();
        let text = format!("{config:?}");

        // Assert
        assert!(config.sentry_enabled);
        assert!(config.traces_enabled);
        assert_eq!(
            config.headers.get("Authorization").map(String::as_str),
            Some(token)
        );
        assert!(!text.contains(dsn));
        assert!(!text.contains("public@"));
        assert!(!text.contains(token));
        assert!(!text.contains("hidden"));
        assert!(text.contains("sentry_dsn"));
        assert!(text.contains("set"));
    }

    #[test]
    fn given_each_parser_branch_when_resolved_then_labels_and_errors_are_exact() {
        // Arrange
        let flags = TelemetryFlags {
            otel_protocol: Some("grpc".to_owned()),
            otel_sampler: Some("traceidratio".to_owned()),
            ..TelemetryFlags::default()
        };
        let getenv = env(&[
            ("OTEL_SDK_DISABLED", "off"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://otel-collector:4318"),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2s"),
            ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
            ("OTEL_RESOURCE_ATTRIBUTES", "service.namespace=reports"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "  "),
            ("SENTRY_ENVIRONMENT", "   "),
            ("SENTRY_DSN", "https://public@sentry.example/1"),
        ]);

        // Act
        let config = resolve_telemetry(&flags, getenv, "2.0.0-alpha.1").unwrap();

        // Assert
        assert_eq!(config.protocol.as_str(), "grpc");
        assert_eq!(config.sampler.as_str(), "traceidratio");
        assert_eq!(OtlpProtocol::HttpProtobuf.as_str(), "http/protobuf");
        assert_eq!(SamplerName::AlwaysOn.as_str(), "always_on");
        assert_eq!(SamplerName::AlwaysOff.as_str(), "always_off");
        assert_eq!(
            SamplerName::ParentBasedAlwaysOn.as_str(),
            "parentbased_always_on"
        );
        assert_eq!(
            SamplerName::ParentBasedAlwaysOff.as_str(),
            "parentbased_always_off"
        );
        assert_eq!(
            SamplerName::ParentBasedTraceIdRatio.as_str(),
            "parentbased_traceidratio"
        );
        assert_eq!(config.sampler_arg, 0.25);
        assert_eq!(config.timeout, Duration::from_secs(2));
        assert_eq!(config.traces_label(), "on");
        assert_eq!(config.sentry_label(), "on");
        assert!(config.header_string().is_empty());
        assert_eq!(config.sentry_environment.as_deref(), Some("   "));
        assert!(
            resolve_telemetry(&TelemetryFlags::default(), env(&[]), "")
                .unwrap_err()
                .to_string()
                .contains("SENTRY_RELEASE")
        );
        assert_eq!(
            config
                .resource_attributes
                .get("service.namespace")
                .map(String::as_str),
            Some("reports")
        );

        let flags = TelemetryFlags::default();
        let invalid = [
            &[("OTEL_SDK_DISABLED", "maybe")][..],
            &[("OTEL_EXPORTER_OTLP_ENDPOINT", "ftp://otel-collector")][..],
            &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel collector")][..],
            &[("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")][..],
            &[("OTEL_TRACES_SAMPLER", "always_off")][..],
            &[("OTEL_TRACES_SAMPLER", "parentbased_always_off")][..],
            &[("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")][..],
            &[("OTEL_TRACES_SAMPLER", "other")][..],
            &[("OTEL_TRACES_SAMPLER_ARG", "2")][..],
            &[("OTEL_TRACES_SAMPLER_ARG", "nan")][..],
            &[("OTEL_EXPORTER_OTLP_TIMEOUT", "0ms")][..],
            &[("OTEL_EXPORTER_OTLP_TIMEOUT", "15")][..],
            &[("OTEL_EXPORTER_OTLP_TIMEOUT", "nope")][..],
            &[("OTEL_EXPORTER_OTLP_HEADERS", "a=b,,c=d")][..],
            &[("OTEL_EXPORTER_OTLP_HEADERS", "orphan")][..],
            &[("OTEL_EXPORTER_OTLP_HEADERS", "=missing")][..],
            &[("SENTRY_DSN", "not-a-dsn")][..],
            &[("OTEL_SERVICE_NAME", "")][..],
        ];
        for pairs in invalid {
            let error = resolve_telemetry(&flags, env(pairs), "2.0.0-alpha.1");
            if pairs[0].0 == "OTEL_EXPORTER_OTLP_PROTOCOL" && pairs[0].1 == "http/protobuf" {
                assert!(error.is_ok());
                continue;
            }
            if pairs[0].0 == "OTEL_TRACES_SAMPLER"
                && matches!(
                    pairs[0].1,
                    "always_off" | "parentbased_always_off" | "parentbased_traceidratio"
                )
            {
                assert!(error.is_ok());
                continue;
            }
            if pairs[0].0 == "OTEL_EXPORTER_OTLP_TIMEOUT" && pairs[0].1 == "15" {
                assert!(error.is_ok());
                continue;
            }
            if pairs[0].0 == "OTEL_SERVICE_NAME" {
                assert!(error.is_ok());
                continue;
            }
            assert!(error.is_err(), "{pairs:?}");
        }
        let headers = resolve_telemetry(
            &flags,
            env(&[("OTEL_EXPORTER_OTLP_HEADERS", "a=b, c=d")]),
            "2.0.0-alpha.1",
        )
        .unwrap();
        assert_eq!(headers.header_string(), "a=b,c=d");
        assert!(
            resolve_telemetry(
                &flags,
                env(&[("OTEL_RESOURCE_ATTRIBUTES", "orphan")]),
                "2.0.0-alpha.1",
            )
            .is_err()
        );
        assert!(
            resolve_telemetry(
                &flags,
                env(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "ftp://otel-collector")]),
                "2.0.0-alpha.1",
            )
            .is_err()
        );
        assert!(
            resolve_telemetry(&flags, env(&[("SENTRY_SAMPLE_RATE", "2")]), "2.0.0-alpha.1",)
                .is_err()
        );
        let odd = TelemetryConfig {
            endpoint: Some("otel-collector".to_owned()),
            ..config
        };
        assert!(format!("{odd:?}").contains("<redacted>"));
    }
}
