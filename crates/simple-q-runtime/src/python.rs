//! Importable module `simple_q._runtime`.
//!
//! The module body only registers classes. It does not open Redis.
//! Each `Client` owns its connection. The process keeps one Tokio runtime.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use redis::aio::ConnectionManager;
use serde_json::Value;

use crate::config::Config;
use crate::job::{Job, new_message_id};
use crate::redis_api;

static RUNTIME: OnceLock<&'static tokio::runtime::Runtime> = OnceLock::new();

pub fn adopt_runtime(runtime: &'static tokio::runtime::Runtime) {
    let _ = RUNTIME.set(runtime);
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(runtime);
}

fn ensure_runtime() -> &'static tokio::runtime::Runtime {
    if let Some(runtime) = RUNTIME.get() {
        return runtime;
    }
    let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime"),
    ));
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(runtime);
    let _ = RUNTIME.set(runtime);
    RUNTIME.get().expect("runtime installed")
}

pub struct ClientState {
    pub url: String,
    pub config: Config,
    connection: tokio::sync::Mutex<Option<ConnectionManager>>,
    on_send: Mutex<Vec<Py<PyAny>>>,
}

impl ClientState {
    pub async fn connect(&self) -> anyhow::Result<ConnectionManager> {
        let mut guard = self.connection.lock().await;
        if let Some(conn) = guard.clone() {
            return Ok(conn);
        }
        let conn = redis_api::connect(&self.url).await?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    pub async fn current(&self) -> Option<ConnectionManager> {
        self.connection.lock().await.clone()
    }
}

/// One Redis connection and the `on_send` hooks for that connection.
#[pyclass]
#[derive(Clone)]
pub struct Client {
    state: Arc<ClientState>,
}

impl Client {
    pub fn state(&self) -> Arc<ClientState> {
        Arc::clone(&self.state)
    }
}

#[pymethods]
impl Client {
    #[new]
    fn new(url: String, config: PyRef<'_, Config>) -> Self {
        Self {
            state: Arc::new(ClientState {
                url,
                config: config.clone(),
                connection: tokio::sync::Mutex::new(None),
                on_send: Mutex::new(Vec::new()),
            }),
        }
    }

    #[getter]
    fn config(&self) -> Config {
        self.state.config.clone()
    }

    fn on_send(&self, py: Python<'_>, hook: Py<PyAny>) -> Py<PyAny> {
        self.state
            .on_send
            .lock()
            .expect("on_send hooks")
            .push(hook.clone_ref(py));
        hook
    }

    #[pyo3(signature = (actor, **kwargs))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        actor: Bound<'py, PyAny>,
        kwargs: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kwargs = kwargs.unwrap_or_else(|| PyDict::new(py));
        let prepared = prepare(py, &self.state, &actor, &kwargs, None, None, None)?;
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let message_id = enqueue_prepared(state, prepared).await?;
            message_id.ok_or_else(|| PyRuntimeError::new_err("send did not enqueue a message"))
        })
    }

    #[pyo3(signature = (actor, *, kwargs=None, delay=None, headers=None, unique_key=None))]
    fn send_with_options<'py>(
        &self,
        py: Python<'py>,
        actor: Bound<'py, PyAny>,
        kwargs: Option<Bound<'py, PyDict>>,
        delay: Option<u64>,
        headers: Option<Bound<'py, PyDict>>,
        unique_key: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kwargs = kwargs.unwrap_or_else(|| PyDict::new(py));
        let prepared = prepare(
            py,
            &self.state,
            &actor,
            &kwargs,
            delay,
            headers.as_ref(),
            unique_key,
        )?;
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            enqueue_prepared(state, prepared).await
        })
    }

    fn worker_alive<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let current = state.current().await;
            let mut conn = match current {
                Some(conn) => conn,
                None => return Ok(false),
            };
            redis_api::worker_alive(Some(&mut conn), &state.config, &name)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            *state.connection.lock().await = None;
            Ok(())
        })
    }

    fn acquire_lock<'py>(
        &self,
        py: Python<'py>,
        key: String,
        ttl_seconds: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let conn = state
                .connect()
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            crate::lock::acquire(conn, key, ttl_seconds)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    fn release_lock<'py>(
        &self,
        py: Python<'py>,
        key: String,
        owner: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let conn = state
                .connect()
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let released = crate::lock::release(conn, key, owner)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Ok(released)
        })
    }
}

struct Prepared {
    job: Job,
    delay: Option<Duration>,
}

fn prepare(
    py: Python<'_>,
    state: &ClientState,
    actor: &Bound<'_, PyAny>,
    kwargs: &Bound<'_, PyDict>,
    delay_ms: Option<u64>,
    headers: Option<&Bound<'_, PyDict>>,
    unique_key: Option<String>,
) -> PyResult<Prepared> {
    if let Some(key) = unique_key.as_deref() {
        if key.is_empty() {
            return Err(PyValueError::new_err("unique_key must not be empty"));
        }
    }
    let actor_name: String = actor.getattr("name")?.extract()?;
    let queue: String = actor.getattr("queue")?.extract()?;
    let empty = PyDict::new(py);
    let mut header_map = string_dict(headers.unwrap_or(&empty))?;
    for hook in state.on_send.lock().expect("on_send hooks").iter() {
        let current = dict_from_headers(py, &header_map)?;
        let updated = hook.call1(py, (actor_name.as_str(), kwargs, current))?;
        if updated.is_none(py) {
            continue;
        }
        let updated = updated
            .bind(py)
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("on_send hook must return a headers dict"))?;
        header_map = string_dict(updated)?;
    }
    let kwargs_json: String = py
        .import("json")?
        .call_method1("dumps", (kwargs,))?
        .extract()?;
    let kwargs_value: Value = serde_json::from_str(&kwargs_json)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    if !kwargs_value.is_object() {
        return Err(PyTypeError::new_err("kwargs must be a dict"));
    }
    let lock_key: Option<String> = actor.getattr("lock_key")?.extract()?;
    let job = Job {
        version: crate::names::MESSAGE_VERSION,
        message_id: new_message_id(),
        actor_name,
        queue,
        kwargs: kwargs_value,
        headers: header_map,
        retries: 0,
        max_retries: actor.getattr("max_retries")?.extract()?,
        time_limit_ms: actor.getattr("time_limit")?.extract()?,
        min_backoff_ms: actor.getattr("min_backoff")?.extract()?,
        max_backoff_ms: actor.getattr("max_backoff")?.extract()?,
        max_deliveries: actor.getattr("max_deliveries")?.extract()?,
        lock_key,
        lock_ttl: actor.getattr("lock_ttl")?.extract()?,
        skip_on_lock: actor.getattr("skip_on_lock")?.extract()?,
        unique_key,
    };
    job.validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let delay = delay_ms
        .filter(|delay| *delay > 0)
        .map(Duration::from_millis);
    Ok(Prepared { job, delay })
}

fn string_dict(dict: &Bound<'_, PyDict>) -> PyResult<BTreeMap<String, String>> {
    let mut headers = BTreeMap::new();
    for (key, value) in dict.iter() {
        let key: String = key
            .extract()
            .map_err(|_| PyTypeError::new_err("headers must be dict[str, str]"))?;
        let value: String = value
            .extract()
            .map_err(|_| PyTypeError::new_err("headers must be dict[str, str]"))?;
        headers.insert(key, value);
    }
    Ok(headers)
}

fn dict_from_headers<'py>(
    py: Python<'py>,
    headers: &BTreeMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in headers {
        dict.set_item(key, value)?;
    }
    Ok(dict)
}

async fn enqueue_prepared(state: Arc<ClientState>, prepared: Prepared) -> PyResult<Option<String>> {
    let mut conn = state
        .connect()
        .await
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    redis_api::enqueue_job(&mut conn, &state.config, prepared.job, prepared.delay)
        .await
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

/// Read-only view of queues and supervisor processes for one [`Client`].
#[pyclass]
pub struct Admin {
    state: Arc<ClientState>,
}

#[pymethods]
impl Admin {
    #[new]
    fn new(client: PyRef<'_, Client>) -> Self {
        Self {
            state: client.state(),
        }
    }

    /// JSON object for `queues`. `host` limits the process tree to that machine.
    #[pyo3(signature = (queues, host=None))]
    fn snapshot<'py>(
        &self,
        py: Python<'py>,
        queues: Vec<String>,
        host: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = state
                .connect()
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let snapshot =
                crate::admin::snapshot(&mut conn, &state.config, &queues, host.as_deref())
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            serde_json::to_string(&snapshot)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// JSON page of dead messages. `limit` defaults to 50, rejects 0, and caps at 200.
    #[pyo3(signature = (queue, limit=50, cursor=None))]
    fn dead<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        limit: i64,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = state
                .connect()
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let page = crate::pages::dead_page(
                &mut conn,
                &state.config,
                &queue,
                page_limit(limit)?,
                cursor.as_deref(),
            )
            .await
            .map_err(page_error)?;
            serde_json::to_string(&page).map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// JSON page of `active` (`LRANGE`) or `scheduled` (`ZRANGE`) tasks.
    #[pyo3(signature = (queue, state, limit=50, cursor=None))]
    fn peek<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        state: String,
        limit: i64,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let runtime_state = Arc::clone(&self.state);
        let _runtime = ensure_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = runtime_state
                .connect()
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let page = crate::pages::peek_page(
                &mut conn,
                &runtime_state.config,
                &queue,
                &state,
                page_limit(limit)?,
                cursor.as_deref(),
            )
            .await
            .map_err(page_error)?;
            serde_json::to_string(&page).map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }
}

fn page_limit(limit: i64) -> PyResult<u64> {
    if limit <= 0 {
        return Err(PyValueError::new_err("limit must be greater than zero"));
    }
    u64::try_from(limit)
        .map(|limit| limit.min(crate::pages::PAGE_MAX))
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

fn page_error(error: anyhow::Error) -> PyErr {
    let text = page_error_text(&error);
    if page_error_is_value(&text) {
        PyValueError::new_err(text)
    } else {
        PyRuntimeError::new_err(text)
    }
}

/// `{:#}` keeps the source chain. `Display` on anyhow is only the outer context.
fn page_error_text(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

fn page_error_is_value(text: &str) -> bool {
    text.contains("limit must")
        || text.contains("cursor is not valid")
        || text.contains("cursor is not in the active list")
        || text.contains("state must")
}

#[cfg(test)]
mod tests {
    use super::{page_error_is_value, page_error_text};

    #[test]
    fn a_wrapped_cursor_error_is_a_value_error() {
        let error = anyhow::anyhow!("cursor is not in the active list")
            .context("failed to page the active list");
        assert!(!page_error_is_value(&error.to_string()));
        let text = page_error_text(&error);
        assert!(page_error_is_value(&text), "{text}");
        assert!(text.contains("cursor is not in the active list"));
    }
}

#[pyfunction]
fn execution_snapshot() -> Vec<String> {
    crate::registry::lines()
}

/// Embedded and wheel entry point.
///
/// The body only registers classes. It does not open Redis or start Tokio.
#[pymodule]
#[pyo3(name = "_runtime", module = "simple_q")]
pub fn simple_q_runtime(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.setattr("__embedded__", false)?;
    module.add_class::<Config>()?;
    module.add_class::<Client>()?;
    module.add_class::<Admin>()?;
    module.add_function(wrap_pyfunction!(execution_snapshot, module)?)?;
    Ok(())
}
