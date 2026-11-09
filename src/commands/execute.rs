use anyhow::{Context, Result};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::controller::Controller;

pub fn run(app_path: &str, task_name: &str, task_id: &str) -> Result<()> {
    let result = Python::attach(|py| -> PyResult<()> {
        let app = load_controller(py, app_path)?;

        let payload = PyDict::new(py);
        payload.set_item("task_id", task_id)?;

        let coroutine = app.call_method1("execute", (task_name, payload))?;

        let result = py.import("asyncio")?.call_method1("run", (coroutine,))?;

        println!("Result: {}", result.repr()?.to_str()?);

        Ok(())
    });

    result
        .map_err(|error| {
            Python::attach(|py| error.print(py));
            anyhow::anyhow!(error.to_string())
        })
        .context("Controller execution failed")
}

fn load_controller<'py>(py: Python<'py>, app_path: &str) -> PyResult<Bound<'py, PyAny>> {
    let (module_name, attribute) = app_path
        .split_once(':')
        .filter(|(module, attribute)| !module.is_empty() && !attribute.is_empty())
        .ok_or_else(|| PyValueError::new_err("Expected application path: module:attribute"))?;

    let app = py.import(module_name)?.getattr(attribute)?;

    if !app.is_instance_of::<Controller>() {
        return Err(PyTypeError::new_err(format!(
            "{app_path} must be an instance of Controller"
        )));
    }

    Ok(app)
}
