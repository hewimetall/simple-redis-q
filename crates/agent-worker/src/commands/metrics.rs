use anyhow::{Context, Result};
use pyo3::prelude::*;

pub fn run() -> Result<()> {
    Python::attach(|py| -> PyResult<()> {
        py.import("simple_q.exposition")?.call_method0("serve")?;
        Ok(())
    })
    .map_err(|error| {
        Python::attach(|py| error.print(py));
        anyhow::anyhow!(error.to_string())
    })
    .context("metrics server failed")
}
