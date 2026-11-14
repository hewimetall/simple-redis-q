"""Prometheus metrics with the same names as Dramatiq and the accounting of
``dramatiq_prometheus_fix``.

``queue_name`` is always canonical: a ``.DQ`` or ``.XQ`` suffix is removed.
Both gauges use ``multiprocess_mode="livesum"``. A retry increments
``dramatiq_message_retries_total`` once, on the canonical queue, when the
retry is enqueued with a delay. A delayed message waits in Redis, so
``dramatiq_delayed_messages_inprogress`` stays at zero.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any

DURATION_BUCKETS = (
    5,
    10,
    25,
    50,
    75,
    100,
    250,
    500,
    750,
    1000,
    2500,
    5000,
    7500,
    10000,
    30000,
    60000,
    600000,
    900000,
    float("inf"),
)

_LABELS = ["queue_name", "actor_name"]
_READY = False
_LOCK = threading.Lock()
_STARTED: dict[str, float] = {}
_METRICS: dict[str, Any] = {}


def q_name(queue_name: str) -> str:
    if queue_name.endswith(".DQ") or queue_name.endswith(".XQ"):
        return queue_name[:-3]
    return queue_name


def install_slot_identity() -> None:
    """Use the worker slot as the prometheus file id before any metric exists."""

    slot = os.environ.get("SIMPLE_Q_SLOT")
    path = (
        os.environ.get("DRAMATIQ_PROM_DB")
        or os.environ.get("dramatiq_prom_db")
        or os.environ.get("PROMETHEUS_MULTIPROC_DIR")
        or os.environ.get("prometheus_multiproc_dir")
    )
    if not slot or not path:
        return
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = path
    os.environ["prometheus_multiproc_dir"] = path
    os.environ["DRAMATIQ_PROM_DB"] = path
    os.environ["dramatiq_prom_db"] = path
    os.makedirs(path, exist_ok=True)
    from simple_q.multiproc import write_slot_pid

    write_slot_pid(path, int(slot))
    from prometheus_client import values

    values.ValueClass = values.MultiProcessValue(lambda: int(slot))


def setup(*, multiprocess: bool | None = None) -> None:
    global _READY
    if _READY:
        return
    if multiprocess is None:
        multiprocess = bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR") or os.environ.get("DRAMATIQ_PROM_DB"))
    if multiprocess:
        install_slot_identity()
    import prometheus_client as prom

    registry = prom.CollectorRegistry()
    _METRICS["total"] = prom.Counter(
        "dramatiq_messages_total",
        "The total number of messages processed.",
        _LABELS,
        registry=registry,
    )
    _METRICS["errors"] = prom.Counter(
        "dramatiq_message_errors_total",
        "The total number of errored messages.",
        _LABELS,
        registry=registry,
    )
    _METRICS["retries"] = prom.Counter(
        "dramatiq_message_retries_total",
        "The total number of retried messages.",
        _LABELS,
        registry=registry,
    )
    _METRICS["rejects"] = prom.Counter(
        "dramatiq_message_rejects_total",
        "The total number of dead-lettered messages.",
        _LABELS,
        registry=registry,
    )
    _METRICS["inprogress"] = prom.Gauge(
        "dramatiq_messages_inprogress",
        "The number of messages in progress.",
        _LABELS,
        registry=registry,
        multiprocess_mode="livesum",
    )
    _METRICS["delayed"] = prom.Gauge(
        "dramatiq_delayed_messages_inprogress",
        "The number of delayed messages in memory.",
        _LABELS,
        registry=registry,
        multiprocess_mode="livesum",
    )
    _METRICS["duration"] = prom.Histogram(
        "dramatiq_message_duration_milliseconds",
        "The time spent processing messages.",
        _LABELS,
        buckets=DURATION_BUCKETS,
        registry=registry,
    )
    _METRICS["registry"] = registry
    _READY = True


def _labels(queue: str, actor: str) -> tuple[str, str]:
    setup()
    return (q_name(queue), actor)


def note_retry(queue: str, actor: str) -> None:
    """One retry enqueue that carries a delay. The canonical queue is the label."""

    setup()
    _METRICS["retries"].labels(*_labels(queue, actor)).inc()


def start_attempt(message_id: str, queue: str, actor: str) -> None:
    """Begin processing. This does not touch the delayed gauge, which stays at zero."""

    setup()
    _METRICS["inprogress"].labels(*_labels(queue, actor)).inc()
    with _LOCK:
        _STARTED[message_id] = time.monotonic()


def finish_attempt(
    message_id: str,
    queue: str,
    actor: str,
    *,
    error: bool = False,
    rejected: bool = False,
) -> None:
    setup()
    labels = _labels(queue, actor)
    with _LOCK:
        started = _STARTED.pop(message_id, None)
    elapsed_ms = 0.0 if started is None else (time.monotonic() - started) * 1000
    _METRICS["duration"].labels(*labels).observe(elapsed_ms)
    _METRICS["inprogress"].labels(*labels).dec()
    _METRICS["total"].labels(*labels).inc()
    if error:
        _METRICS["errors"].labels(*labels).inc()
    if rejected:
        _METRICS["rejects"].labels(*labels).inc()


def reject(queue: str, actor: str) -> None:
    setup()
    _METRICS["rejects"].labels(*_labels(queue, actor)).inc()
