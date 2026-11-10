use pyo3::prelude::*;
use simple_q_runtime::python::simple_q_runtime as embedded_runtime;

pub fn initialize() {
    pyo3::append_to_inittab!(embedded_runtime);
    Python::initialize();
    Python::attach(|py| {
        let module = py
            .import("simple_q_runtime")
            .expect("simple_q_runtime is registered on the inittab");
        let file = module.getattr("__file__").ok();
        if let Some(file) = file.filter(|value| !value.is_none()) {
            if let Ok(path) = file.extract::<String>() {
                if path.ends_with(".so") || path.ends_with(".pyd") {
                    panic!("simple_q_runtime was loaded from {path}, not the inittab");
                }
            }
        }
        module
            .setattr("__embedded__", true)
            .expect("mark the inittab module");
    });
}
