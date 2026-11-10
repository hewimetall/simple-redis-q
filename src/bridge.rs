use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyList, PyTuple};
use pyo3_async_runtimes::TaskLocals;
use serde_json::{Map, Value};

use crate::job::Job;
use crate::registry;

static LOCALS: OnceLock<TaskLocals> = OnceLock::new();
static APP: OnceLock<Py<PyAny>> = OnceLock::new();
static PING_SCHEDULE_STUCK: AtomicBool = AtomicBool::new(false);

pub fn start(app_path: &str) -> Result<()> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let app_path = app_path.to_owned();

    std::thread::Builder::new()
        .name("python-asyncio".to_owned())
        .spawn(move || {
            let started = Python::attach(|py| -> PyResult<()> {
                let asyncio = py.import("asyncio")?;
                let event_loop = asyncio.call_method0("new_event_loop")?;
                asyncio.call_method1("set_event_loop", (&event_loop,))?;
                LOCALS
                    .set(TaskLocals::new(event_loop.clone()))
                    .expect("event loop locals installed once");
                let app = load_controller(py, &app_path)?;
                APP.set(app.unbind()).expect("controller installed once");
                sender.send(()).expect("loop start signal");
                event_loop.call_method0("run_forever")?;
                Ok(())
            });
            if let Err(error) = started {
                Python::attach(|py| error.print(py));
                let _ = sender.send(());
            }
        })
        .context("failed to start the Python loop thread")?;

    receiver
        .recv()
        .context("Python loop thread exited before signalling")?;
    if LOCALS.get().is_none() || APP.get().is_none() {
        anyhow::bail!("Python loop did not publish a controller");
    }
    Ok(())
}

pub fn load_controller<'py>(py: Python<'py>, app_path: &str) -> PyResult<Bound<'py, PyAny>> {
    let (module_name, attribute) = app_path
        .split_once(':')
        .filter(|(module, attribute)| !module.is_empty() && !attribute.is_empty())
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("Expected application path: module:attribute")
        })?;

    let app = py.import(module_name)?.getattr(attribute)?;
    if !app.is_instance_of::<crate::controller::Controller>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{app_path} must be an instance of Controller"
        )));
    }
    Ok(app)
}

pub async fn execute(queue: &str, job: &Job, task_id: &str) -> Result<Value> {
    let execution_id = format!("agent-worker:{queue}:{task_id}:{}", job.task);
    registry::insert(execution_id.clone(), job.task.clone(), queue.to_owned());
    let result = execute_tracked(&execution_id, job).await;
    registry::remove(&execution_id);
    result
}

async fn execute_tracked(execution_id: &str, job: &Job) -> Result<Value> {
    let locals = LOCALS.get().context("Python loop is not running")?;
    let execution_id = execution_id.to_owned();
    let task = job.task.clone();
    let payload = job.payload.clone();

    let awaitable: Py<PyAny> = Python::attach(|py| -> PyResult<Py<PyAny>> {
        let app = APP
            .get()
            .context("controller is not loaded")
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        let dict = json_to_dict(py, &payload)?;
        let coroutine = app.bind(py).call_method1("execute", (task, dict))?;
        let tracked = py
            .import("simple_q.tracking")?
            .call_method1("tracked", (coroutine, execution_id))?;
        Ok(tracked.unbind())
    })
    .context("failed to build the Python coroutine")?;

    let future = Python::attach(|py| {
        pyo3_async_runtimes::into_future_with_locals(locals, awaitable.into_bound(py))
    })
    .context("failed to schedule the Python coroutine")?;

    let result = future.await.context("Python task failed")?;
    Python::attach(|py| py_to_json(py, result.bind(py))).context("failed to encode the task result")
}

pub async fn ping(timeout: Duration) -> bool {
    if PING_SCHEDULE_STUCK.swap(true, Ordering::SeqCst) {
        return false;
    }

    let flag = Arc::new(AtomicBool::new(false));
    let scheduled_flag = Arc::clone(&flag);
    let scheduled = tokio::task::spawn_blocking(move || {
        let result = Python::attach(|py| -> PyResult<()> {
            let event_loop = LOCALS
                .get()
                .ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Python loop is not running")
                })?
                .event_loop(py);
            let callback_flag = Arc::clone(&scheduled_flag);
            let callback = PyCFunction::new_closure(py, None, None, move |_args, _kwargs| {
                callback_flag.store(true, Ordering::SeqCst);
                Ok::<_, PyErr>(())
            })?;
            event_loop.call_method1("call_soon_threadsafe", (callback,))?;
            Ok(())
        });
        PING_SCHEDULE_STUCK.store(false, Ordering::SeqCst);
        result.is_ok()
    });

    let scheduled = match tokio::time::timeout(timeout, scheduled).await {
        Ok(Ok(ready)) => ready,
        _ => return false,
    };
    if !scheduled {
        return false;
    }

    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if flag.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

pub fn request_cancel(execution_id: &str) {
    let execution_id = execution_id.to_owned();
    let _ = Python::attach(|py| -> PyResult<()> {
        let event_loop = LOCALS
            .get()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Python loop is not running"))?
            .event_loop(py);
        let name = execution_id;
        let callback =
            PyCFunction::new_closure(py, None, None, move |_args, _kwargs| -> PyResult<()> {
                let py = _args.py();
                cancel_named(py, &name)
            })?;
        event_loop.call_method1("call_soon_threadsafe", (callback,))?;
        Ok(())
    });
}

fn cancel_named(py: Python<'_>, name: &str) -> PyResult<()> {
    let tasks = py.import("asyncio")?.call_method0("all_tasks")?;
    for task in tasks.try_iter()? {
        let task = task?;
        let task_name: String = task.call_method0("get_name")?.extract()?;
        if task_name == name {
            task.call_method0("cancel")?;
        }
    }
    Ok(())
}

fn json_to_dict<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyDict>> {
    let object = match value {
        Value::Object(entries) => entries,
        _ => {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "task payload must be a JSON object",
            ));
        }
    };
    let dict = PyDict::new(py);
    for (key, item) in object {
        dict.set_item(key, json_to_py(py, item)?)?;
    }
    Ok(dict)
}

fn json_to_py<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    match value {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(value) => Ok(pyo3::types::PyBool::new(py, *value).to_owned().into_any()),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(value.into_pyobject(py)?.into_any())
            } else if let Some(value) = value.as_u64() {
                Ok(value.into_pyobject(py)?.into_any())
            } else if let Some(value) = value.as_f64() {
                Ok(value.into_pyobject(py)?.into_any())
            } else {
                Err(pyo3::exceptions::PyTypeError::new_err(
                    "unsupported JSON number",
                ))
            }
        }
        Value::String(value) => Ok(value.into_pyobject(py)?.into_any()),
        Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(json_to_py(py, value)?)?;
            }
            Ok(list.into_any())
        }
        Value::Object(entries) => Ok(json_object_to_dict(py, entries)?.into_any()),
    }
}

fn json_object_to_dict<'py>(
    py: Python<'py>,
    entries: &Map<String, Value>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in entries {
        dict.set_item(key, json_to_py(py, value)?)?;
    }
    Ok(dict)
}

fn py_to_json(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        return Ok(Value::Null);
    }
    let json = py.import("json")?;
    let dumps = json.getattr("dumps")?;
    let kwargs = PyDict::new(py);
    let default =
        PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _kwargs| {
            Ok::<String, PyErr>(args.get_item(0)?.str()?.to_string())
        })?;
    kwargs.set_item("default", default)?;
    let encoded: String = dumps.call((value,), Some(&kwargs))?.extract()?;
    serde_json::from_str(&encoded).map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!("task result is not JSON: {error}"))
    })
}

pub fn snapshot_lines() -> Vec<String> {
    registry::lines()
}
