use std::path::PathBuf;

use anyhow::{Context, Result};
use pyo3_stub_gen::StubInfo;

pub fn run() -> Result<()> {
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");

    let info = StubInfo::from_project_root(
        "simple_q_runtime".to_owned(),
        output.clone(),
        true,
        Default::default(),
    )
    .context("Failed to collect stub metadata")?;

    info.generate().context("Failed to generate Python stubs")?;

    println!("Generated stubs in {}", output.display());

    Ok(())
}
