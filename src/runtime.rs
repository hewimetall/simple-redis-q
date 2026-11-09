use pyo3::prelude::*;

use crate::controller::simple_q_runtime;

pub fn initialize() {
    pyo3::append_to_inittab!(simple_q_runtime);
    Python::initialize();
}
