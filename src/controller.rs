use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

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

#[pymodule]
pub fn simple_q_runtime(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Controller>()?;
    Ok(())
}
