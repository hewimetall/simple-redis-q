//! Parent configuration. This crate does not initialize CPython.

mod parent;
mod redis_endpoint;
mod telemetry;

pub use parent::{
    PARENT_NO_CPYTHON_IMPLEMENTED, ParentBootstrap, ParentConfigSource, ParentInitError,
};
pub use redis_endpoint::{RedisEndpoint, parse_redis_endpoint};
pub use telemetry::{
    OtlpProtocol, SamplerName, TelemetryConfig, TelemetryError, TelemetryFlags, resolve_telemetry,
};
