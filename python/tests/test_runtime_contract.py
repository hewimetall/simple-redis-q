import asyncio
import logging
import random
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q.backoff import compute_backoff
from simple_q import worker_name
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


@pytest.fixture(autouse=True)
def _restore_embedded_runtime_module():
    previous = sys.modules.get("simple_q_runtime")
    try:
        yield
    finally:
        if previous is None:
            sys.modules.pop("simple_q_runtime", None)
        else:
            sys.modules["simple_q_runtime"] = previous


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


def test_registry_tracing_defaults_to_none():
    from simple_q import Tracing

    assert Registry().tracing is None
    assert Registry(tracing=None).tracing is None
    assert Tracing is not None


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


def test_last_exception_calls_on_dead_once():
    registry, _seen = _registry()
    calls = []

    def hook(actor, kwargs, reason, message_id, retries):
        calls.append((actor, kwargs, reason, message_id, retries))

    registry.on_dead(hook)
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
    assert calls == [("discover", {"value": "boom"}, "retries", "dead-1", 0)]


def test_timeout_on_the_last_attempt_calls_on_dead():
    registry, _seen = _registry()
    reasons = []
    registry.on_dead(
        lambda actor, kwargs, reason, message_id, retries: reasons.append(
            (actor, reason, kwargs, message_id, retries)
        )
    )
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
    assert reasons == [("discover", "timeout", {"value": "slow"}, "slow-1", 0)]


def test_retries_hook_receives_the_exception_text():
    registry, _seen = _registry()
    calls = []

    async def hook(actor, kwargs, reason, message_id, retries, *, error=None):
        calls.append((actor, kwargs, reason, message_id, retries, error))

    registry.on_dead(hook)
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=0,
        message_id="dead-error",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "retries"}
    assert calls == [
        ("discover", {"value": "boom"}, "retries", "dead-error", 0, "RuntimeError: boom")
    ]


def test_timeout_hook_receives_the_exception_text():
    registry, _seen = _registry()
    calls = []

    def hook(actor, kwargs, reason, message_id, retries, error=None):
        calls.append((actor, reason, message_id, retries, error))

    registry.on_dead(hook)
    message = build_message(
        actor_name="discover",
        queue="default",
        kwargs={"value": "slow"},
        headers={},
        max_retries=0,
        time_limit_ms=20,
        message_id="slow-error",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "timeout"}
    assert calls == [("discover", "timeout", "slow-error", 0, "TimeoutError: ")]


def test_poison_hook_receives_none():
    registry, _seen = _registry()
    calls = []

    def hook(actor, kwargs, reason, message_id, retries, *, error=None):
        calls.append((reason, message_id, retries, error))

    registry.on_dead(hook)
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "ok"},
        headers={},
        max_retries=0,
        max_deliveries=2,
        message_id="poison-error",
    )
    message["deliveries"] = 3
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "poison"}
    assert calls == [("poison", "poison-error", 0, None)]


def test_unknown_actor_and_invalid_hooks_receive_none():
    registry, _seen = _registry()
    calls = []

    def hook(actor, kwargs, reason, message_id, retries, **extra):
        calls.append((reason, message_id, extra["error"]))

    registry.on_dead(hook)
    unknown = build_message(
        actor_name="missing",
        queue="quota",
        kwargs={"value": 1},
        headers={},
        max_retries=0,
        message_id="missing-error",
        retries=2,
    )
    asyncio.run(registry.dispatch(unknown))
    asyncio.run(registry.note_dead("discover", {"value": 1}, "invalid", "bad-error", 0))
    assert calls == [
        ("unknown_actor", "missing-error", None),
        ("invalid", "bad-error", None),
    ]


def test_five_argument_dead_hook_still_runs():
    registry, _seen = _registry()
    calls = []

    def hook(actor, kwargs, reason, message_id, retries):
        calls.append((actor, kwargs, reason, message_id, retries))

    registry.on_dead(hook)
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=0,
        message_id="dead-five",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "retries"}
    assert calls == [("discover", {"value": "boom"}, "retries", "dead-five", 0)]


def test_wrapped_five_argument_hook_still_runs_without_error():
    import functools

    registry, _seen = _registry()
    calls = []

    def inner(actor, kwargs, reason, message_id, retries):
        calls.append((reason, message_id))

    @functools.wraps(inner)
    def hook(*args, **kwargs):
        if "error" in kwargs:
            raise AssertionError("five-argument hook was given error")
        return inner(*args)

    registry.on_dead(hook)
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=0,
        message_id="dead-wrapped",
    )
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "retries"}
    assert calls == [("retries", "dead-wrapped")]


def test_poison_delivery_calls_on_dead_and_rejects():
    registry, seen = _registry()
    reasons = []
    registry.on_dead(
        lambda actor, kwargs, reason, message_id, retries: reasons.append(
            (reason, message_id, retries)
        )
    )
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
    assert reasons == [("poison", "poison-1", 0)]
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
    runtime = types.ModuleType("simple_q_runtime")

    async def acquire_lock(key, ttl):
        return "owner-1"

    async def release_lock(key, owner):
        raise RuntimeError("redis down")

    runtime.acquire_lock = acquire_lock
    runtime.release_lock = release_lock
    sys.modules["simple_q_runtime"] = runtime
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


def _dead_hook(calls):
    def hook(actor, kwargs, reason, message_id, retries):
        calls.append((actor, kwargs, reason, message_id, retries))

    return hook


def test_unknown_actor_calls_on_dead_once():
    registry, _seen = _registry()
    calls = []
    registry.on_dead(_dead_hook(calls))
    message = build_message(
        actor_name="missing",
        queue="quota",
        kwargs={"value": 1},
        headers={},
        max_retries=0,
        message_id="missing-1",
        retries=2,
    )
    result = asyncio.run(registry.dispatch(message))
    assert result == {"outcome": "dead", "reason": "unknown_actor"}
    assert calls == [("missing", {"value": 1}, "unknown_actor", "missing-1", 2)]


def test_invalid_message_hook_is_called_once():
    registry = Registry()
    calls = []
    registry.on_dead(_dead_hook(calls))
    asyncio.run(registry.note_dead("discover", {"value": 1}, "invalid", "bad-1", 0))
    assert calls == [("discover", {"value": 1}, "invalid", "bad-1", 0)]


def test_each_dead_reason_notifies_the_hook_once_per_event():
    registry, _seen = _registry()
    calls = []
    registry.on_dead(_dead_hook(calls))

    retry_message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=0,
        message_id="once-retries",
    )
    timeout_message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "slow"},
        headers={},
        max_retries=0,
        time_limit_ms=20,
        message_id="once-timeout",
    )
    poison_message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "ok"},
        headers={},
        max_retries=0,
        max_deliveries=1,
        message_id="once-poison",
    )
    poison_message["deliveries"] = 2
    unknown_message = build_message(
        actor_name="missing",
        queue="quota",
        kwargs={},
        headers={},
        max_retries=0,
        message_id="once-unknown",
    )
    asyncio.run(registry.dispatch(retry_message))
    asyncio.run(registry.dispatch(timeout_message))
    asyncio.run(registry.dispatch(poison_message))
    asyncio.run(registry.dispatch(unknown_message))
    asyncio.run(registry.note_dead("discover", {}, "invalid", "once-invalid", 4))
    reasons = [call[2] for call in calls]
    assert reasons == ["retries", "timeout", "poison", "unknown_actor", "invalid"]
    assert len(calls) == len(set((call[2], call[3]) for call in calls))


def test_traceparent_set_at_send_is_visible_in_the_handler():
    import contextvars

    from simple_q import disable_memory, drain, enable_memory

    traceparent = contextvars.ContextVar("traceparent", default="")
    registry = Registry()
    seen = {}

    def stamp(actor_name, kwargs, headers):
        assert traceparent.get() == "00-from-caller"
        updated = dict(headers)
        updated["traceparent"] = traceparent.get()
        return updated

    async def around(message, call_next):
        seen["around"] = current_message().headers["traceparent"]
        return await call_next()

    registry.on_send(stamp)
    registry.around_dispatch(around)

    @registry.actor(name="traced", queue="default", max_retries=0)
    async def traced():
        seen["handler"] = current_message().headers["traceparent"]
        return "ok"

    enable_memory()
    try:
        token = traceparent.set("00-from-caller")
        try:
            asyncio.run(traced.send())
        finally:
            traceparent.reset(token)
        asyncio.run(drain("default", registry))
    finally:
        disable_memory()
    assert seen == {"around": "00-from-caller", "handler": "00-from-caller"}


def test_memory_drain_runs_messages_sent_by_a_handler():
    from simple_q import disable_memory, drain, enable_memory

    registry = Registry()
    seen = []

    @registry.actor(name="parent", queue="default", max_retries=0)
    async def parent():
        seen.append("parent")
        await child.send(value=2)

    @registry.actor(name="child", queue="default", max_retries=0)
    async def child(value: int):
        seen.append(value)

    enable_memory()
    try:
        asyncio.run(parent.send())
        asyncio.run(drain("default", registry))
    finally:
        disable_memory()
    assert seen == ["parent", 2]


def test_lock_key_template_is_filled_from_kwargs():
    runtime = types.ModuleType("simple_q_runtime")
    seen = {}

    async def acquire_lock(key, ttl):
        seen["key"] = key
        return "owner"

    async def release_lock(key, owner):
        return True

    runtime.acquire_lock = acquire_lock
    runtime.release_lock = release_lock
    previous = sys.modules.get("simple_q_runtime")
    sys.modules["simple_q_runtime"] = runtime
    try:
        registry = Registry()

        @registry.actor(name="calc", queue="default", lock_key="cache:calc:{project_name}", max_retries=0)
        async def calc(project_name: str):
            return project_name

        message = build_message(
            actor_name="calc",
            queue="default",
            kwargs={"project_name": "alpha"},
            headers={},
            max_retries=0,
            lock_key="cache:calc:{project_name}",
            message_id="lock-1",
        )
        result = asyncio.run(registry.dispatch(message))
        assert result["outcome"] == "ok"
        assert seen["key"] == "cache:calc:alpha"
    finally:
        if previous is None:
            sys.modules.pop("simple_q_runtime", None)
        else:
            sys.modules["simple_q_runtime"] = previous


def test_lock_key_template_rejects_a_missing_placeholder():
    registry = Registry()

    @registry.actor(name="calc", queue="default", lock_key="cache:calc:{project_name}", max_retries=0)
    async def calc(project_name: str = "x"):
        return project_name

    message = build_message(
        actor_name="calc",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        lock_key="cache:calc:{project_name}",
        message_id="lock-missing",
    )
    try:
        asyncio.run(registry.dispatch(message))
    except KeyError as error:
        assert "project_name" in str(error)
    else:
        raise AssertionError("missing lock_key placeholder was accepted")


def test_lock_key_rejects_a_non_canonical_character():
    from simple_q.registry import usable_lock_key

    assert usable_lock_key("cache:calc:alpha")
    assert not usable_lock_key("cache:acme\u200b")
    registry = Registry()

    @registry.actor(name="calc", queue="default", lock_key="cache:calc:{project_name}", max_retries=0)
    async def calc(project_name: str):
        return project_name

    message = build_message(
        actor_name="calc",
        queue="default",
        kwargs={"project_name": "acme\u200b"},
        headers={},
        max_retries=0,
        lock_key="cache:calc:{project_name}",
        message_id="lock-zwsp",
    )
    try:
        asyncio.run(registry.dispatch(message))
    except ValueError as error:
        assert "not a usable server key" in str(error)
    else:
        raise AssertionError("a zero-width lock key was accepted")


def test_current_message_carries_deliveries_and_the_worker_name():
    assert worker_name() is None
    registry = Registry()
    seen = {}

    @registry.actor(name="marked", queue="default", max_retries=0)
    async def marked():
        seen["name"] = worker_name()
        seen["deliveries"] = current_message().deliveries
        seen["field"] = current_message().worker_name

    message = build_message(
        actor_name="marked",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        message_id="marked-1",
    )
    message["deliveries"] = 4
    message["worker_name"] = "agent-worker.default.host.7.abcd"
    result = asyncio.run(registry.dispatch(message))
    assert result["outcome"] == "ok"
    assert seen == {
        "name": "agent-worker.default.host.7.abcd",
        "deliveries": 4,
        "field": "agent-worker.default.host.7.abcd",
    }
    assert worker_name() is None

    @registry.actor(name="plain", queue="default", max_retries=0)
    async def plain():
        seen["plain_name"] = worker_name()
        seen["plain_deliveries"] = current_message().deliveries

    plain_message = build_message(
        actor_name="plain",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        message_id="plain-1",
    )
    asyncio.run(registry.dispatch(plain_message))
    assert seen["plain_name"] is None
    assert seen["plain_deliveries"] == 1
