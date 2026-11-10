use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyTuple};
use pyo3_async_runtimes::TaskLocals;
use serde_json::Value;

use simple_q_runtime::job::Job;
use simple_q_runtime::registry;

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
    if app.getattr("dispatch").is_err() {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{app_path} must be a Registry with dispatch"
        )));
    }
    Ok(app)
}

pub async fn call_hook(name: &str) -> Result<()> {
    let locals = LOCALS.get().context("Python loop is not running")?;
    let name = name.to_owned();
    let awaitable: Py<PyAny> = Python::attach(|py| -> PyResult<Py<PyAny>> {
        let app = APP
            .get()
            .context("registry is not loaded")
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        let app = app.bind(py);
        if app.getattr(&name).is_err() {
            return Ok(py.None());
        }
        let coroutine = app.call_method0(name)?;
        if coroutine.is_none() {
            return Ok(py.None());
        }
        Ok(coroutine.unbind())
    })?;
    if Python::attach(|py| awaitable.bind(py).is_none()) {
        return Ok(());
    }
    let future = Python::attach(|py| {
        pyo3_async_runtimes::into_future_with_locals(locals, awaitable.into_bound(py))
    })?;
    future.await.context("registry hook failed")?;
    Ok(())
}

pub async fn execute(job: &Job, task_id: &str, deliveries: u64) -> Result<Value> {
    let execution_id = format!("agent-worker:{}:{task_id}:{}", job.queue, job.actor_name);
    registry::insert(
        execution_id.clone(),
        job.actor_name.clone(),
        job.queue.clone(),
    );
    let result = execute_tracked(&execution_id, job, deliveries).await;
    registry::remove(&execution_id);
    result
}

async fn execute_tracked(execution_id: &str, job: &Job, deliveries: u64) -> Result<Value> {
    let locals = LOCALS.get().context("Python loop is not running")?;
    let execution_id = execution_id.to_owned();
    let mut message = serde_json::to_value(job).context("message is not JSON")?;
    message["deliveries"] = Value::from(deliveries);
    let message_json = serde_json::to_string(&message).context("message is not JSON")?;

    let awaitable: Py<PyAny> = Python::attach(|py| -> PyResult<Py<PyAny>> {
        let app = APP
            .get()
            .context("registry is not loaded")
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        let parsed = py.import("json")?.call_method1("loads", (message_json,))?;
        let coroutine = app.bind(py).call_method1("dispatch", (parsed,))?;
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

pub fn note_retry(queue: &str, actor: &str) {
    let queue = queue.to_owned();
    let actor = actor.to_owned();
    let recorded = Python::attach(|py| -> PyResult<()> {
        py.import("simple_q.metrics")?
            .call_method1("note_retry", (queue, actor))?;
        Ok(())
    });
    if let Err(error) = recorded {
        tracing::warn!("retry metric was not recorded: {error}");
    }
}

pub async fn note_dead(job: &Job, reason: &str) -> Result<()> {
    let locals = LOCALS.get().context("Python loop is not running")?;
    let reason = reason.to_owned();
    let actor_name = job.actor_name.clone();
    let message_id = job.message_id.clone();
    let retries = job.retries;
    let kwargs = if job.kwargs.is_object() {
        job.kwargs.clone()
    } else {
        Value::Object(Default::default())
    };
    let awaitable: Py<PyAny> = Python::attach(|py| -> PyResult<Py<PyAny>> {
        let app = APP
            .get()
            .context("registry is not loaded")
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        let payload = json_to_py(py, &kwargs)?;
        let coroutine = app.bind(py).call_method1(
            "note_dead",
            (actor_name, payload, reason, message_id, retries),
        )?;
        Ok(coroutine.unbind())
    })
    .context("failed to build the dead-letter hook")?;
    let future = Python::attach(|py| {
        pyo3_async_runtimes::into_future_with_locals(locals, awaitable.into_bound(py))
    })
    .context("failed to schedule the dead-letter hook")?;
    future.await.context("dead-letter hook failed")?;
    Ok(())
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

fn json_to_py<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

    match value {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(item) => Ok(PyBool::new(py, *item).to_owned().into_any()),
        Value::Number(number) => {
            if let Some(item) = number.as_i64() {
                Ok(PyInt::new(py, item).into_any())
            } else if let Some(item) = number.as_u64() {
                Ok(PyInt::new(py, item).into_any())
            } else if let Some(item) = number.as_f64() {
                Ok(PyFloat::new(py, item).into_any())
            } else {
                Ok(PyString::new(py, &number.to_string()).into_any())
            }
        }
        Value::String(item) => Ok(PyString::new(py, item).into_any()),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        Value::Object(items) => {
            let dict = PyDict::new(py);
            for (key, item) in items {
                dict.set_item(key, json_to_py(py, item)?)?;
            }
            Ok(dict.into_any())
        }
    }
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
