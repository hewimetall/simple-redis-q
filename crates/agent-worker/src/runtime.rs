use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};

use pyo3::prelude::*;
use simple_q_runtime::python::simple_q_runtime as embedded_runtime;

const EMBEDDED_NAME: &CStr = c"simple_q._runtime";

pub fn initialize() {
    pyo3::append_to_inittab!(embedded_runtime);
    unsafe {
        pyo3::ffi::PyImport_AppendInittab(
            EMBEDDED_NAME.as_ptr(),
            Some(embedded_runtime::__pyo3_init),
        );
    }
    init_interpreter();
    Python::attach(|py| {
        let module = load_embedded(py);
        let modules = py
            .import("sys")
            .expect("sys")
            .getattr("modules")
            .expect("sys.modules");
        modules
            .set_item("simple_q._runtime", &module)
            .expect("register simple_q._runtime");
        let _ = modules.del_item("_runtime");
        module
            .setattr("__name__", "simple_q._runtime")
            .expect("name the embedded module");
        let module = py
            .import("simple_q._runtime")
            .expect("simple_q._runtime is registered on the inittab");
        let file = module.getattr("__file__").ok();
        if let Some(file) = file.filter(|value| !value.is_none())
            && let Ok(path) = file.extract::<String>()
            && (path.ends_with(".so") || path.ends_with(".pyd"))
        {
            panic!("simple_q._runtime was loaded from {path}, not the inittab");
        }
        module
            .setattr("__embedded__", true)
            .expect("mark the inittab module");
    });
}

fn load_embedded(py: Python<'_>) -> Bound<'_, PyAny> {
    let imp = py.import("_imp").expect("_imp");
    let machinery = py
        .import("importlib.machinery")
        .expect("importlib.machinery");
    for name in ["simple_q._runtime", "_runtime"] {
        let spec = machinery
            .call_method1("ModuleSpec", (name, py.None()))
            .expect("module spec");
        if let Ok(module) = imp.call_method1("create_builtin", (spec,))
            && !module.is_none()
        {
            imp.call_method1("exec_builtin", (&module,))
                .expect("execute the inittab module");
            return module;
        }
    }
    panic!("simple_q._runtime is registered on the inittab");
}

fn init_interpreter() {
    // PyO3's Python::initialize calls Py_InitializeEx, which does not point
    // sys.executable at this binary. site.py only adds a virtualenv's
    // site-packages when pyvenv.cfg sits next to that executable or one
    // directory above it. Set the config path from the launch path when that
    // file is present, and leave the default config alone otherwise so the
    // shared libpython still finds the standard library.
    //
    // PyInitConfig (3.14) defaults to an isolated runtime and would skip
    // site.py. PyConfig and Py_InitializeFromConfig stay available through
    // 3.15 and are what loads the virtualenv. The binary links the GIL
    // libpython for one minor version. Free-threaded builds are not linked.
    let venv_executable = venv_executable();
    unsafe {
        if pyo3::ffi::Py_IsInitialized() != 0 {
            return;
        }
        if let Some(executable) = venv_executable.as_deref()
            && init_from_executable(executable)
        {
            return;
        }
        pyo3::ffi::Py_InitializeEx(0);
        pyo3::ffi::PyEval_SaveThread();
    }
}

fn init_from_executable(executable: &str) -> bool {
    let Ok(executable) = CString::new(executable) else {
        return false;
    };
    unsafe {
        let mut config = std::mem::zeroed();
        pyo3::ffi::PyConfig_InitPythonConfig(&mut config);
        config.install_signal_handlers = 0;
        let named = pyo3::ffi::PyConfig_SetBytesString(
            &mut config,
            &mut config.program_name,
            executable.as_ptr(),
        );
        if pyo3::ffi::PyStatus_Exception(named) != 0 {
            pyo3::ffi::PyConfig_Clear(&mut config);
            return false;
        }
        let located = pyo3::ffi::PyConfig_SetBytesString(
            &mut config,
            &mut config.executable,
            executable.as_ptr(),
        );
        if pyo3::ffi::PyStatus_Exception(located) != 0 {
            pyo3::ffi::PyConfig_Clear(&mut config);
            return false;
        }
        let status = pyo3::ffi::Py_InitializeFromConfig(&config);
        pyo3::ffi::PyConfig_Clear(&mut config);
        if pyo3::ffi::PyStatus_Exception(status) != 0 {
            return false;
        }
        pyo3::ffi::PyEval_SaveThread();
    }
    true
}

fn venv_executable() -> Option<String> {
    let mut candidates = Vec::new();
    if let Some(argv0) = std::env::args().next() {
        let path = PathBuf::from(&argv0);
        if path.is_absolute() {
            candidates.push(path);
        } else if path.components().nth(1).is_some()
            && let Ok(cwd) = std::env::current_dir()
        {
            candidates.push(cwd.join(path));
        }
    }
    if let Ok(path) = std::env::current_exe() {
        candidates.push(path);
    }
    candidates
        .into_iter()
        .find(|path| pyvenv_near(path))
        .and_then(|path| path.into_os_string().into_string().ok())
}

fn pyvenv_near(executable: &Path) -> bool {
    let Some(directory) = executable.parent() else {
        return false;
    };
    if directory.join("pyvenv.cfg").is_file() {
        return true;
    }
    directory
        .parent()
        .is_some_and(|parent| parent.join("pyvenv.cfg").is_file())
}
