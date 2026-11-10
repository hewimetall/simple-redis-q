use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::job::Job;
use crate::redis_api;

static RUNTIME: OnceLock<&'static tokio::runtime::Runtime> = OnceLock::new();
static INSTALL_LOCK: Mutex<()> = Mutex::new(());

pub fn adopt_runtime(runtime: &'static tokio::runtime::Runtime) {
    let _ = RUNTIME.set(runtime);
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(runtime);
}

fn ensure_runtime() -> &'static tokio::runtime::Runtime {
    if let Some(runtime) = RUNTIME.get() {
        return runtime;
    }
    let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime"),
    ));
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(runtime);
    let _ = RUNTIME.set(runtime);
    RUNTIME.get().expect("runtime installed")
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    ensure_runtime().block_on(future)
}

/// Embedded and wheel entry point.
///
/// The body only registers callables. It does not open Redis or start Tokio.
/// The worker binary sets `__embedded__` after inittab registration.
#[pymodule]
pub fn simple_q_runtime(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.setattr("__embedded__", false)?;
    module.add_function(wrap_pyfunction!(install, module)?)?;
    module.add_function(wrap_pyfunction!(is_configured, module)?)?;
    module.add_function(wrap_pyfunction!(enqueue, module)?)?;
    module.add_function(wrap_pyfunction!(acquire_lock, module)?)?;
    module.add_function(wrap_pyfunction!(release_lock, module)?)?;
    module.add_function(wrap_pyfunction!(execution_snapshot, module)?)?;
    Ok(())
}

#[pyfunction]
fn install(url: String) -> PyResult<()> {
    let _guard = INSTALL_LOCK.lock().expect("install lock");
    block_on(redis_api::install(url)).map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

#[pyfunction]
fn is_configured() -> bool {
    redis_api::is_configured()
}

#[pyfunction]
fn enqueue<'py>(
    py: Python<'py>,
    message_json: String,
    delay_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let value = serde_json::from_str(&message_json)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let job = Job::parse(value).map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let delay = delay_ms.map(Duration::from_millis);
        let message_id = redis_api::enqueue_job(job, delay)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(message_id)
    })
}

#[pyfunction]
fn acquire_lock<'py>(
    py: Python<'py>,
    key: String,
    ttl_seconds: u64,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        crate::lock::acquire(key, ttl_seconds)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })
}

#[pyfunction]
fn release_lock<'py>(py: Python<'py>, key: String, owner: String) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let released = crate::lock::release(key, owner)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(released)
    })
}

#[pyfunction]
fn execution_snapshot() -> Vec<String> {
    crate::registry::lines()
}
