import asyncio
import logging
import random
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q.backoff import compute_backoff
from simple_q.message import current_message
from simple_q.metrics import (
    DURATION_BUCKETS,
    finish_attempt,
    hold_delay,
    note_retry,
    q_name,
    reject,
    release_delay,
    setup,
    start_attempt,
    value,
)
from simple_q.registry import MESSAGE_VERSION, Registry, build_message


def test_backoff_matches_dramatiq_1_18_ranges():
    rng = random.Random(0)
    for _ in range(200):
        first = compute_backoff(0, min_backoff=15_000, max_backoff=86_400_000, rng=rng)
        second = compute_backoff(1, min_backoff=15_000, max_backoff=86_400_000, rng=rng)
        third = compute_backoff(2, min_backoff=15_000, max_backoff=86_400_000, rng=rng)
        assert 7_500 <= first < 15_000
        assert 15_000 <= second < 30_000
        assert 30_000 <= third < 60_000


def test_queue_labels_are_canonical():
    assert q_name("default") == "default"
    assert q_name("quota.DQ") == "quota"
    assert q_name("default.XQ") == "default"


def test_metric_definitions_follow_the_fixed_middleware():
    setup(multiprocess=False)
    from simple_q.metrics import _METRICS

    assert _METRICS["inprogress"]._multiprocess_mode == "livesum"
    assert _METRICS["delayed"]._multiprocess_mode == "livesum"
    assert _METRICS["delayed"]._documentation == "The number of delayed messages in memory."
    assert _METRICS["duration"]._kwargs["buckets"] == DURATION_BUCKETS
    assert DURATION_BUCKETS[-1] == float("inf")


def test_dispatch_does_not_count_a_retry_before_enqueue():
    setup(multiprocess=False)
    registry, _seen = _registry()
    before = value("retries", "quota", "discover")
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=2,
        min_backoff_ms=15_000,
        max_backoff_ms=86_400_000,
        message_id="count-after-enqueue",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result["outcome"] == "retry"
    assert value("retries", "quota", "discover") == before


def test_retry_is_counted_once_on_the_canonical_queue():
    setup(multiprocess=False)
    note_retry("quota.DQ", "discover")
    assert value("retries", "quota", "discover") >= 1
    assert value("delayed", "quota", "discover") == 0
    assert value("retries", "quota.DQ", "discover") == value("retries", "quota", "discover")


def test_in_memory_delay_returns_to_zero_and_processing_does_not_touch_it():
    setup(multiprocess=False)
    before = value("delayed", "default", "discover")
    hold_delay("m-1", "default.DQ", "discover")
    hold_delay("m-1", "default", "discover")
    assert value("delayed", "default", "discover") == before + 1
    start_attempt("m-1", "default", "discover")
    assert value("delayed", "default", "discover") == before + 1
    finish_attempt("m-1", "default", "discover")
    release_delay("m-1")
    release_delay("m-1")
    assert value("delayed", "default", "discover") == before


def test_skip_counts_as_processed_without_an_error_and_reject_is_separate():
    setup(multiprocess=False)
    total = value("total", "default", "gated")
    errors = value("errors", "default", "gated")
    start_attempt("skip-1", "default", "gated")
    finish_attempt("skip-1", "default", "gated", error=False, rejected=False)
    assert value("total", "default", "gated") == total + 1
    assert value("errors", "default", "gated") == errors
    rejects = value("rejects", "quota", "discover")
    reject("quota", "discover")
    assert value("rejects", "quota", "discover") == rejects + 1


def test_message_version_and_headers_are_separate_from_kwargs():
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": 1},
        headers={"trace": "abc"},
        max_retries=3,
    )
    assert message["version"] == MESSAGE_VERSION == 1
    assert message["headers"] == {"trace": "abc"}
    assert "trace" not in message["kwargs"]
    assert message["time_limit_ms"] == 1_800_000


def test_send_before_configure_fails_clearly():
    runtime = types.ModuleType("simple_q_runtime")
    runtime.is_configured = lambda: False

    async def enqueue(*args):
        raise AssertionError("enqueue must not run")

    runtime.enqueue = enqueue
    sys.modules["simple_q_runtime"] = runtime
    registry = Registry()

    @registry.actor(name="discover", queue="default")
    async def discover(value: int):
        return value

    try:
        asyncio.run(discover.send(value=1))
    except RuntimeError as error:
        assert "configure" in str(error)
    else:
        raise AssertionError("send without configure succeeded")


def test_import_does_not_open_the_runtime_module_by_itself():
    sys.modules.pop("simple_q_runtime", None)
    import simple_q.backoff as backoff
    import simple_q.registry as registry

    assert "simple_q_runtime" not in sys.modules
    assert backoff.compute_backoff is not None
    assert registry.Registry is not None


def _registry():
    registry = Registry()
    seen = []

    @registry.actor(
        name="discover",
        queue="quota",
        max_retries=2,
        time_limit=50,
        min_backoff=15_000,
        max_backoff=86_400_000,
        max_deliveries=2,
    )
    async def discover(value: str):
        seen.append((value, current_message().asdict()))
        if value == "boom":
            raise RuntimeError("boom")
        if value == "slow":
            await asyncio.sleep(1)
        return {"ok": True}

    return registry, seen


def test_retry_then_success_keeps_one_message_id_and_reports_retries():
    registry, seen = _registry()
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={"trace": "t"},
        max_retries=2,
        min_backoff_ms=15_000,
        max_backoff_ms=86_400_000,
        message_id="same-id",
    )
    first = asyncio.run(registry.dispatch(message))
    assert first["outcome"] == "retry"
    assert 7_500 <= first["delay_ms"] < 15_000
    message["retries"] = 1
    message["kwargs"] = {"value": "ok"}
    second = asyncio.run(registry.dispatch(message))
    assert second["outcome"] == "ok"
    assert seen[0][1]["message_id"] == "same-id"
    assert seen[0][1]["retries"] == 0
    assert seen[1][1]["retries"] == 1
    assert seen[0][1]["headers"]["trace"] == "t"
    assert "actor_name" in seen[0][1] and "kwargs" in seen[0][1]


def test_last_exception_is_dead_without_the_dead_hook():
    registry, _seen = _registry()
    reasons = []
    registry.on_dead(lambda actor, kwargs, reason: reasons.append(reason))
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=0,
        message_id="dead-1",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "retries"}
    assert reasons == []


def test_timeout_on_the_last_attempt_calls_on_dead():
    registry, _seen = _registry()
    reasons = []
    registry.on_dead(lambda actor, kwargs, reason: reasons.append((actor, reason, kwargs)))
    message = build_message(
        actor_name="discover",
        queue="default",
        kwargs={"value": "slow"},
        headers={},
        max_retries=0,
        time_limit_ms=20,
        message_id="slow-1",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result["outcome"] == "dead"
    assert result["reason"] == "timeout"
    assert reasons == [("discover", "timeout", {"value": "slow"})]


def test_poison_delivery_calls_on_dead_and_rejects():
    registry, seen = _registry()
    reasons = []
    registry.on_dead(lambda actor, kwargs, reason: reasons.append(reason))
    before = value("rejects", "quota", "discover")
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "ok"},
        headers={},
        max_retries=0,
        max_deliveries=2,
        message_id="poison-1",
    )
    message["deliveries"] = 3
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "poison"}
    assert reasons == ["poison"]
    assert seen == []
    assert value("rejects", "quota", "discover") == before + 1


def test_lock_skip_counts_as_a_processed_message_without_an_error():
    runtime = types.ModuleType("simple_q_runtime")

    async def acquire_lock(key, ttl):
        return None

    async def release_lock(key, owner):
        raise AssertionError("release without an owner")

    runtime.acquire_lock = acquire_lock
    runtime.release_lock = release_lock
    sys.modules["simple_q_runtime"] = runtime
    registry = Registry()

    @registry.actor(name="gated", queue="default", lock_key="demo:gate", skip_on_lock=True)
    async def gated():
        raise AssertionError("handler ran")

    before_total = value("total", "default", "gated")
    before_errors = value("errors", "default", "gated")
    message = build_message(
        actor_name="gated",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        lock_key="demo:gate",
        skip_on_lock=True,
        message_id="skip-1",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result["outcome"] == "skipped"
    assert value("total", "default", "gated") == before_total + 1
    assert value("errors", "default", "gated") == before_errors


def test_release_failure_keeps_a_successful_result():
    runtime = sys.modules["simple_q_runtime"]

    async def acquire_lock(key, ttl):
        return "owner-1"

    async def release_lock(key, owner):
        raise RuntimeError("redis down")

    runtime.acquire_lock = acquire_lock
    runtime.release_lock = release_lock
    registry = Registry()

    @registry.actor(name="gated", queue="default", lock_key="demo:gate")
    async def gated():
        return {"ok": True}

    message = build_message(
        actor_name="gated",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        lock_key="demo:gate",
        message_id="rel-1",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result["outcome"] == "ok"
    assert result["result"] == {"ok": True}


def test_hook_exceptions_are_logged(caplog=None):
    registry = Registry()

    def broken():
        raise RuntimeError("hook")

    registry.on_start(broken)
    records = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = ListHandler()
    logging.getLogger("simple_q").addHandler(handler)
    try:
        asyncio.run(registry.startup())
    finally:
        logging.getLogger("simple_q").removeHandler(handler)
    assert any("start hook failed" in record.getMessage() for record in records)


def test_scrape_reaps_a_dead_slot_and_keeps_a_live_one():
    import os
    import tempfile

    from simple_q.multiproc import reap_dead_processes, write_slot_pid

    with tempfile.TemporaryDirectory() as directory:
        live = os.path.join(directory, "gauge_livesum_4.db")
        dead = os.path.join(directory, "gauge_livesum_9.db")
        open(live, "wb").close()
        open(dead, "wb").close()
        write_slot_pid(directory, 4, os.getpid())
        write_slot_pid(directory, 9, 1 << 22)
        reaped = reap_dead_processes(directory)
        assert 9 in reaped
        assert 4 not in reaped
        assert os.path.exists(live)
        assert not os.path.exists(dead)
        assert not os.path.exists(os.path.join(directory, "slot_9.pid"))
