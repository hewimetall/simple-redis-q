use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::lock;
use crate::redis_api;

#[gen_stub_pyclass]
#[pyclass(subclass, module = "simple_q_runtime")]
pub struct Controller;

#[gen_stub_pymethods]
#[pymethods]
impl Controller {
    #[new]
    fn new() -> Self {
        Self
    }

    fn execute(&self, task_name: &str, payload: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let _ = (task_name, payload);

        Err(PyNotImplementedError::new_err(
            "Override async execute(self, task_name, payload)",
        ))
    }

    fn next(
        &self,
        task_name: &str,
        payload: &Bound<'_, PyDict>,
        result: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (task_name, payload, result);

        Err(PyNotImplementedError::new_err(
            "Override async next(self, task_name, payload, result)",
        ))
    }

    fn err(
        &self,
        task_name: &str,
        payload: &Bound<'_, PyDict>,
        error: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (task_name, payload, error);

        Err(PyNotImplementedError::new_err(
            "Override async err(self, task_name, payload, error)",
        ))
    }
}

#[pyfunction]
fn enqueue<'py>(
    py: Python<'py>,
    queue: String,
    task: String,
    payload_json: String,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        redis_api::enqueue(queue, task, payload_json)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(())
    })
}

#[pyfunction]
fn acquire_lock<'py>(
    py: Python<'py>,
    key: String,
    ttl_seconds: u64,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        lock::acquire(key, ttl_seconds)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })
}

#[pyfunction]
fn release_lock<'py>(py: Python<'py>, key: String, owner: String) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let released = lock::release(key, owner)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(released)
    })
}

/// Rust-side execution records. Reading them does not call into asyncio.
#[pyfunction]
fn execution_snapshot() -> Vec<String> {
    crate::bridge::snapshot_lines()
}

#[pymodule]
pub fn simple_q_runtime(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Controller>()?;
    module.add_function(wrap_pyfunction!(enqueue, module)?)?;
    module.add_function(wrap_pyfunction!(acquire_lock, module)?)?;
    module.add_function(wrap_pyfunction!(release_lock, module)?)?;
    module.add_function(wrap_pyfunction!(execution_snapshot, module)?)?;
    Ok(())
}
