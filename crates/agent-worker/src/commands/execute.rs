use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::bridge::load_app;

pub fn run(app_path: &str, factory: bool, task_name: &str, task_id: &str) -> Result<()> {
    let result = Python::attach(|py| -> PyResult<()> {
        let app = load_app(py, app_path, factory)?;
        let actors = app.getattr("actors")?;
        let actor = actors.get_item(task_name)?;
        let function = actor.getattr("function")?;

        let payload = PyDict::new(py);
        payload.set_item("task_id", task_id)?;
        let coroutine = function.call((), Some(&payload))?;
        let result = py.import("asyncio")?.call_method1("run", (coroutine,))?;
        println!("Result: {}", result.repr()?.to_str()?);
        Ok(())
    });

    result
        .map_err(|error| {
            Python::attach(|py| error.print(py));
            anyhow::anyhow!(error.to_string())
        })
        .context("actor execution failed")
}
