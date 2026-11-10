use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Builder;

use crate::bridge::load_controller;
use crate::redis_api;

pub fn run(app_path: &str, task_name: &str, task_id: &str) -> Result<()> {
    if std::env::var("REDIS_URL").is_ok() {
        let runtime = Builder::new_multi_thread().enable_all().build()?;
        let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(runtime));
        pyo3_async_runtimes::tokio::init_with_runtime(runtime)
            .map_err(|_| anyhow::anyhow!("Tokio runtime was already installed"))?;
        runtime.block_on(redis_api::install_from_env())?;
    }

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
