import asyncio
import importlib.util
import logging
import os
import random
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q import App, DeadLetter, MemoryClient, actor, worker_name
from simple_q.backoff import compute_backoff
from simple_q.dispatch import MESSAGE_VERSION, build_message, usable_lock_key
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


class FakeClient:
    def __init__(self, *, busy=False, release_error=None):
        self.busy = busy
        self.release_error = release_error
        self.keys = []

    async def acquire_lock(self, key, ttl):
        self.keys.append(key)
        if self.busy:
            return None
        return "owner-1"

    async def release_lock(self, key, owner):
        if self.release_error is not None:
            raise self.release_error
        return True


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


def test_dramatiq_metric_names_and_labels_are_frozen():
    setup(multiprocess=False)
    from simple_q.metrics import _METRICS

    frozen = {
        "dramatiq_messages_total": ("queue_name", "actor_name"),
        "dramatiq_message_errors_total": ("queue_name", "actor_name"),
        "dramatiq_message_retries_total": ("queue_name", "actor_name"),
        "dramatiq_message_rejects_total": ("queue_name", "actor_name"),
        "dramatiq_messages_inprogress": ("queue_name", "actor_name"),
        "dramatiq_delayed_messages_inprogress": ("queue_name", "actor_name"),
        "dramatiq_message_duration_milliseconds": ("queue_name", "actor_name"),
    }
    def exported_name(metric) -> str:
        name = metric._name
        if getattr(metric, "_type", "") == "counter":
            return f"{name}_total"
        return name

    found = {
        exported_name(metric): tuple(metric._labelnames)
        for metric in _METRICS.values()
        if getattr(metric, "_name", "").startswith("dramatiq_")
    }
    assert found == frozen


def test_dispatch_does_not_count_a_retry_before_enqueue():
    setup(multiprocess=False)
    app, _seen = _discover()
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
    result = asyncio.run(app.dispatch(message))
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


def test_queue_name_is_one_lowercase_word():
    with pytest.raises(ValueError, match="default"):
        build_message(
            actor_name="discover",
            queue="Quota",
            kwargs={},
            headers={},
            max_retries=0,
        )


def test_app_tracing_defaults_to_none():
    from simple_q import Tracing

    app, _seen = _discover()
    assert app.tracing is None
    assert App(client=FakeClient(), actors=app.actors.values(), tracing=None).tracing is None
    assert Tracing is not None


def test_importing_an_actor_module_does_not_construct_an_app_or_open_redis():
    sys.modules.pop("simple_q._runtime", None)
    sys.modules.pop("actor_only", None)
    path = Path(__file__).resolve().parent / "actor_only.py"
    spec = importlib.util.spec_from_file_location("actor_only", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert "simple_q._runtime" not in sys.modules
    assert not hasattr(module, "App")
    assert module.recalc.name == "recalc"
    assert module.recalc.queue == "default"


def test_a_repeated_actor_name_fails_at_construction():
    first, _seen = _discover()
    again, _seen = _discover()
    with pytest.raises(ValueError, match="already registered"):
        App(client=FakeClient(), actors=[*first.actors.values(), *again.actors.values()])


def _discover():
    seen = []

    @actor(
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

    return App(client=FakeClient(), actors=[discover]), seen


def test_retry_then_success_keeps_one_message_id_and_reports_retries():
    app, seen = _discover()
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
    first = asyncio.run(app.dispatch(message))
    assert first["outcome"] == "retry"
    assert 7_500 <= first["delay_ms"] < 15_000
    message["retries"] = 1
    message["kwargs"] = {"value": "ok"}
    second = asyncio.run(app.dispatch(message))
    assert second["outcome"] == "ok"
    assert seen[0][1]["message_id"] == "same-id"
    assert seen[0][1]["retries"] == 0
    assert seen[1][1]["retries"] == 1
    assert seen[0][1]["headers"]["trace"] == "t"
    assert "actor_name" in seen[0][1] and "kwargs" in seen[0][1]


def _letter(calls):
    def hook(letter: DeadLetter):
        calls.append(letter)

    return hook


def test_last_exception_calls_on_dead_once():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
    message = build_message(
        actor_name="discover",
        queue="quota",
        kwargs={"value": "boom"},
        headers={},
        max_retries=0,
        message_id="dead-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "dead", "reason": "retries", "error": "RuntimeError: boom"}
    assert len(calls) == 1
    letter = calls[0]
    assert letter.actor_name == "discover"
    assert letter.kwargs == {"value": "boom"}
    assert letter.reason == "retries"
    assert letter.message_id == "dead-1"
    assert letter.retries == 0
    assert letter.deliveries == 1
    assert letter.error == "RuntimeError: boom"


def test_timeout_on_the_last_attempt_calls_on_dead():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
    message = build_message(
        actor_name="discover",
        queue="default",
        kwargs={"value": "slow"},
        headers={},
        max_retries=0,
        time_limit_ms=20,
        message_id="slow-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result["outcome"] == "dead"
    assert result["reason"] == "timeout"
    assert result["error"].startswith("TimeoutError:")
    letter = calls[0]
    assert letter.actor_name == "discover"
    assert letter.reason == "timeout"
    assert letter.kwargs == {"value": "slow"}
    assert letter.message_id == "slow-1"
    assert letter.retries == 0
    assert letter.deliveries == 1
    assert letter.error.startswith("TimeoutError:")


def test_poison_hook_receives_none():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
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
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "dead", "reason": "poison", "error": None}
    letter = calls[0]
    assert letter.reason == "poison"
    assert letter.message_id == "poison-error"
    assert letter.retries == 0
    assert letter.deliveries == 3
    assert letter.error is None
    assert letter.actor_name == "discover"
    assert letter.kwargs == {"value": "ok"}


def test_unknown_actor_and_invalid_hooks_receive_none():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
    unknown = build_message(
        actor_name="missing",
        queue="quota",
        kwargs={"value": 1},
        headers={},
        max_retries=0,
        message_id="missing-error",
        retries=2,
    )
    asyncio.run(app.dispatch(unknown))
    asyncio.run(app.note_dead("discover", {"value": 1}, "invalid", "bad-error", 0))
    assert [(letter.reason, letter.message_id, letter.error, letter.deliveries) for letter in calls] == [
        ("unknown_actor", "missing-error", None, 1),
        ("invalid", "bad-error", None, 0),
    ]
    assert calls[0].actor_name == "missing"
    assert calls[0].kwargs == {"value": 1}
    assert calls[0].retries == 2
    assert calls[1].retries == 0


def test_poison_delivery_calls_on_dead_and_rejects():
    app, seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
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
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "dead", "reason": "poison", "error": None}
    assert calls[0].reason == "poison"
    assert calls[0].message_id == "poison-1"
    assert calls[0].error is None
    assert seen == []
    assert value("rejects", "quota", "discover") == before + 1


def test_lock_skip_counts_as_a_processed_message_without_an_error():
    client = FakeClient(busy=True)

    @actor(name="gated", queue="default", lock_key="demo:gate", skip_on_lock=True)
    async def gated():
        raise AssertionError("handler ran")

    app = App(client=client, actors=[gated])
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
    result = asyncio.run(app.dispatch(message))
    assert result["outcome"] == "skipped"
    assert value("total", "default", "gated") == before_total + 1
    assert value("errors", "default", "gated") == before_errors


def test_release_failure_keeps_a_successful_result():
    client = FakeClient(release_error=RuntimeError("redis down"))

    @actor(name="gated", queue="default", lock_key="demo:gate")
    async def gated():
        return {"ok": True}

    app = App(client=client, actors=[gated])
    message = build_message(
        actor_name="gated",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        lock_key="demo:gate",
        message_id="rel-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result["outcome"] == "ok"
    assert result["result"] == {"ok": True}


def test_hook_exceptions_are_logged():
    app, _seen = _discover()

    def broken():
        raise RuntimeError("hook")

    app.on_start(broken)
    records = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = ListHandler()
    logging.getLogger("simple_q").addHandler(handler)
    try:
        asyncio.run(app.startup())
    finally:
        logging.getLogger("simple_q").removeHandler(handler)
    assert any("start hook failed" in record.getMessage() for record in records)


def test_child_memory_scrape_is_zero_without_a_file_and_reads_one_when_present(tmp_path):
    from simple_q.exposition import child_memory_samples

    missing = child_memory_samples(tmp_path).decode()
    assert "simple_q_child_memory_drains_total 0\n" in missing
    assert "simple_q_child_memory_kills_total 0\n" in missing

    prom = tmp_path / "simple_q_child_memory.prom"
    prom.write_text(
        "# TYPE simple_q_child_memory_drains_total counter\n"
        "simple_q_child_memory_drains_total 4\n"
        "simple_q_child_memory_kills_total 2\n",
        encoding="utf-8",
    )
    present = child_memory_samples(tmp_path).decode()
    assert "simple_q_child_memory_drains_total 4\n" in present
    assert "simple_q_child_memory_kills_total 2\n" in present

    prom.write_text("not a sample\n", encoding="utf-8")
    garbage = child_memory_samples(tmp_path).decode()
    assert "simple_q_child_memory_drains_total 0\n" in garbage
    assert "simple_q_child_memory_kills_total 0\n" in garbage


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


def test_unknown_actor_calls_on_dead_once():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
    message = build_message(
        actor_name="missing",
        queue="quota",
        kwargs={"value": 1},
        headers={},
        max_retries=0,
        message_id="missing-1",
        retries=2,
    )
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "dead", "reason": "unknown_actor", "error": None}
    letter = calls[0]
    assert letter.actor_name == "missing"
    assert letter.kwargs == {"value": 1}
    assert letter.reason == "unknown_actor"
    assert letter.message_id == "missing-1"
    assert letter.retries == 2
    assert letter.error is None


def test_invalid_message_hook_is_called_once():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))
    asyncio.run(app.note_dead("discover", {"value": 1}, "invalid", "bad-1", 0))
    letter = calls[0]
    assert letter.actor_name == "discover"
    assert letter.kwargs == {"value": 1}
    assert letter.reason == "invalid"
    assert letter.message_id == "bad-1"
    assert letter.retries == 0
    assert letter.deliveries == 0
    assert letter.error is None


def test_each_dead_reason_notifies_the_hook_once_per_event():
    app, _seen = _discover()
    calls = []
    app.on_dead(_letter(calls))

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
    asyncio.run(app.dispatch(retry_message))
    asyncio.run(app.dispatch(timeout_message))
    asyncio.run(app.dispatch(poison_message))
    asyncio.run(app.dispatch(unknown_message))
    asyncio.run(app.note_dead("discover", {}, "invalid", "once-invalid", 4))
    reasons = [letter.reason for letter in calls]
    assert reasons == ["retries", "timeout", "poison", "unknown_actor", "invalid"]
    assert calls[0].error == "RuntimeError: boom"
    assert calls[1].error.startswith("TimeoutError:")
    assert calls[2].error is None
    assert calls[3].error is None
    assert calls[4].error is None
    assert len(calls) == len({(letter.reason, letter.message_id) for letter in calls})


def test_traceparent_set_at_send_is_visible_in_the_handler():
    import contextvars

    traceparent = contextvars.ContextVar("traceparent", default="")
    client = MemoryClient()
    seen = {}

    def stamp(actor_name, kwargs, headers):
        assert traceparent.get() == "00-from-caller"
        updated = dict(headers)
        updated["traceparent"] = traceparent.get()
        return updated

    async def around(message, call_next):
        seen["around"] = current_message().headers["traceparent"]
        return await call_next()

    client.on_send(stamp)

    @actor(name="traced", queue="default", max_retries=0)
    async def traced():
        seen["handler"] = current_message().headers["traceparent"]
        return "ok"

    app = App(client=client, actors=[traced], around_dispatch=[around])
    token = traceparent.set("00-from-caller")
    try:
        asyncio.run(client.send(traced))
    finally:
        traceparent.reset(token)
    asyncio.run(client.drain(app))
    assert seen == {"around": "00-from-caller", "handler": "00-from-caller"}


def test_memory_unique_key_blocks_a_second_send_until_drain():
    client = MemoryClient()
    seen = []

    @actor(name="recalc", queue="default", max_retries=0)
    async def recalc(project: str):
        seen.append(project)
        return project

    app = App(client=client, actors=[recalc])
    first = asyncio.run(
        client.send_with_options(recalc, kwargs={"project": "42"}, unique_key="recalc:42")
    )
    second = asyncio.run(
        client.send_with_options(recalc, kwargs={"project": "42"}, unique_key="recalc:42")
    )
    assert isinstance(first, str)
    assert second is None
    assert client.depth("default") == 1
    plain = asyncio.run(client.send(recalc, project="42"))
    assert isinstance(plain, str)
    assert client.depth("default") == 2
    asyncio.run(client.drain(app))
    assert seen == ["42", "42"]
    assert client.depth("default") == 0
    third = asyncio.run(
        client.send_with_options(recalc, kwargs={"project": "42"}, unique_key="recalc:42")
    )
    assert isinstance(third, str)
    assert client.depth("default") == 1


def test_memory_drain_runs_messages_sent_by_a_handler():
    client = MemoryClient()
    seen = []

    @actor(name="parent", queue="default", max_retries=0)
    async def parent():
        seen.append("parent")
        await client.send(child, value=2)

    @actor(name="child", queue="default", max_retries=0)
    async def child(value: int):
        seen.append(value)

    app = App(client=client, actors=[parent, child])
    asyncio.run(client.send(parent))
    asyncio.run(client.drain(app))
    assert seen == ["parent", 2]


def test_lock_key_template_is_filled_from_kwargs():
    client = FakeClient()

    @actor(name="calc", queue="default", lock_key="cache:calc:{project_name}", max_retries=0)
    async def calc(project_name: str):
        return project_name

    app = App(client=client, actors=[calc])
    message = build_message(
        actor_name="calc",
        queue="default",
        kwargs={"project_name": "alpha"},
        headers={},
        max_retries=0,
        lock_key="cache:calc:{project_name}",
        message_id="lock-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result["outcome"] == "ok"
    assert client.keys == ["cache:calc:alpha"]


def test_lock_key_template_rejects_a_missing_placeholder():
    @actor(name="calc", queue="default", lock_key="cache:calc:{project_name}", max_retries=0)
    async def calc(project_name: str = "x"):
        return project_name

    app = App(client=FakeClient(), actors=[calc])
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
        asyncio.run(app.dispatch(message))
    except KeyError as error:
        assert "project_name" in str(error)
    else:
        raise AssertionError("missing lock_key placeholder was accepted")


def test_lock_key_rejects_a_non_canonical_character():
    assert usable_lock_key("cache:calc:alpha")
    assert not usable_lock_key("cache:acme\u200b")

    @actor(name="calc", queue="default", lock_key="cache:calc:{project_name}", max_retries=0)
    async def calc(project_name: str):
        return project_name

    app = App(client=FakeClient(), actors=[calc])
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
        asyncio.run(app.dispatch(message))
    except ValueError as error:
        assert "not a usable server key" in str(error)
    else:
        raise AssertionError("a zero-width lock key was accepted")


def test_current_message_carries_deliveries_and_the_worker_name():
    assert worker_name() is None
    app, _seen = _discover()
    seen = {}

    @actor(name="marked", queue="default", max_retries=0)
    async def marked():
        seen["name"] = worker_name()
        seen["deliveries"] = current_message().deliveries
        seen["field"] = current_message().worker_name

    marked_app = App(client=FakeClient(), actors=[*app.actors.values(), marked])
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
    result = asyncio.run(marked_app.dispatch(message))
    assert result["outcome"] == "ok"
    assert seen == {
        "name": "agent-worker.default.host.7.abcd",
        "deliveries": 4,
        "field": "agent-worker.default.host.7.abcd",
    }
    assert worker_name() is None

    @actor(name="plain", queue="default", max_retries=0)
    async def plain():
        seen["plain_name"] = worker_name()
        seen["plain_deliveries"] = current_message().deliveries

    plain_app = App(client=FakeClient(), actors=[plain])
    plain_message = build_message(
        actor_name="plain",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        message_id="plain-1",
    )
    asyncio.run(plain_app.dispatch(plain_message))
    assert seen["plain_name"] is None
    assert seen["plain_deliveries"] == 1


def test_queue_admin_stays_unloaded_until_imported():
    import simple_q

    assert "QueueAdmin" in simple_q.__all__
    assert "QueueAdmin" not in vars(simple_q)


def test_memory_client_worker_alive_is_false_until_recorded():
    async def probe():
        client = MemoryClient()
        assert await client.worker_alive("agent-worker.default.host.1.abcd1234") is False
        assert await client.worker_alive("not-a-worker") is False
        client.set_worker_alive("agent-worker.default.host.1.abcd1234")
        assert await client.worker_alive("agent-worker.default.host.1.abcd1234") is True
        client.set_worker_alive("agent-worker.default.host.1.abcd1234", False)
        assert await client.worker_alive("agent-worker.default.host.1.abcd1234") is False

    asyncio.run(probe())


def test_memory_drain_requeues_retry_and_runs_hooks():
    client = MemoryClient()
    seen = []
    hooks = []
    blocked = []

    @actor(name="recalc", queue="default", max_retries=1)
    async def recalc():
        seen.append(current_message().retries)
        if current_message().retries == 0:
            blocked.append(
                await client.send_with_options(recalc, unique_key="recalc:42")
            )
            raise RuntimeError("again")
        return "ok"

    app = App(
        client=client,
        actors=[recalc],
        on_start=[lambda: hooks.append("start")],
        on_stop=[lambda: hooks.append("stop")],
    )
    asyncio.run(client.send_with_options(recalc, unique_key="recalc:42"))
    asyncio.run(client.drain(app))
    assert seen == [0, 1]
    assert hooks == ["start", "stop"]
    assert blocked == [None]
    assert client.depth("default") == 0
    again = asyncio.run(client.send_with_options(recalc, unique_key="recalc:42"))
    assert isinstance(again, str)


def test_memory_drain_timeout_is_cancelled_error_and_then_on_dead():
    client = MemoryClient()
    caught = []
    letters = []

    @actor(name="slow", queue="default", max_retries=1, time_limit=1)
    async def slow():
        try:
            await asyncio.sleep(1)
        except Exception as exc:
            caught.append(type(exc).__name__)

    app = App(client=client, actors=[slow], on_dead=[letters.append])
    asyncio.run(client.send(slow))
    asyncio.run(client.drain(app))
    assert caught == []
    assert [letter.reason for letter in letters] == ["timeout"]
    assert letters[0].error.startswith("TimeoutError:")


def _redis_db(number: int) -> str:
    url = f"redis://127.0.0.1:6379/{number}"
    try:
        probe = subprocess.run(
            ["redis-cli", "-u", url, "PING"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        pytest.skip("Redis is down")
    if probe.returncode != 0 or "PONG" not in probe.stdout:
        pytest.skip("Redis is down")
    return url


def _redis(*args: str, db: int = 15) -> str:
    output = subprocess.run(
        ["redis-cli", "-n", str(db), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return output.stdout


def test_config_from_env_rejects_bad_values(monkeypatch):
    from simple_q import Config

    names = [
        "SIMPLE_Q_HEARTBEAT_SECS",
        "SIMPLE_Q_MISSED_HEARTBEATS",
        "SIMPLE_Q_PING_BUDGET_SECS",
        "SIMPLE_Q_DONE_TTL_SECS",
        "SIMPLE_Q_DEAD_TTL_SECS",
        "SIMPLE_Q_UNIQUE_TTL_SECS",
        "SIMPLE_Q_MAX_DELIVERIES",
        "SIMPLE_Q_MEMORY_SOFT_RATIO",
        "SIMPLE_Q_MEMORY_HARD_RATIO",
        "SIMPLE_Q_CHILD_MEMORY_SOFT",
        "SIMPLE_Q_CHILD_MEMORY_HARD",
    ]
    for name in names:
        monkeypatch.delenv(name, raising=False)
    config = Config.from_env()
    assert config.heartbeat_secs == 15
    assert config.missed_heartbeats == 4
    assert config.unique_ttl_secs == 86400
    assert config.max_deliveries == 5
    assert config.memory_soft_ratio == pytest.approx(0.80)
    assert config.memory_hard_ratio == pytest.approx(0.95)

    monkeypatch.setenv("SIMPLE_Q_HEARTBEAT_SECS", "abc")
    with pytest.raises(ValueError, match="SIMPLE_Q_HEARTBEAT_SECS"):
        Config.from_env()
    monkeypatch.delenv("SIMPLE_Q_HEARTBEAT_SECS")
    monkeypatch.setenv("SIMPLE_Q_MEMORY_SOFT_RATIO", "1.5")
    with pytest.raises(ValueError, match="SIMPLE_Q_MEMORY_SOFT_RATIO"):
        Config.from_env()


def test_two_clients_write_to_their_own_databases():
    from simple_q import Client, Config

    left_url = _redis_db(15)
    right_url = _redis_db(13)
    queue = "splitq"
    for db in (15, 13):
        _redis("DEL", f"{queue}:data", f"{queue}:pending", f"{queue}:scheduled", db=db)

    @actor(queue=queue, max_retries=0)
    async def recalc(project_id: str) -> None:
        return None

    async def send_both():
        left = Client(left_url, config=Config())
        right = Client(right_url, config=Config())
        try:
            await left.send(recalc, project_id="15")
            await right.send(recalc, project_id="13")
        finally:
            await left.close()
            await right.close()

    asyncio.run(send_both())
    left_payload = _redis("HVALS", f"{queue}:data", db=15)
    right_payload = _redis("HVALS", f"{queue}:data", db=13)
    assert '"project_id":"15"' in left_payload
    assert '"project_id":"13"' not in left_payload
    assert '"project_id":"13"' in right_payload
    assert '"project_id":"15"' not in right_payload
    for db in (15, 13):
        _redis("DEL", f"{queue}:data", f"{queue}:pending", f"{queue}:scheduled", db=db)


def test_worker_alive_never_connected_or_bad_name_is_false_and_quiet(caplog):
    from simple_q import Client, Config

    url = _redis_db(15)

    async def probe():
        client = Client(url, config=Config())
        caplog.set_level(logging.DEBUG)
        assert await client.worker_alive("not-a-worker") is False
        assert await client.worker_alive("agent-worker.default.host.1.abcd") is False

        @actor(queue="walive", max_retries=0)
        async def marker(project_id: str) -> None:
            return None

        await client.send(marker, project_id="x")
        assert await client.worker_alive("not-a-worker") is False
        await client.close()
        return True

    assert asyncio.run(probe()) is True
    assert not caplog.records


def test_worker_alive_reads_the_heartbeat_and_lets_the_loop_run():
    from simple_q import Client, Config

    url = _redis_db(15)
    queue = "walive"
    key = f"{queue}:workers"
    fresh = "agent-worker.walive.testhost.11.abcd1234"
    stale = "agent-worker.walive.testhost.12.abcd1234"
    unknown = "agent-worker.walive.testhost.13.abcd1234"
    dotted = "agent-worker.walive.host.example.com.14.abcd1234"
    _redis("DEL", key, db=15)
    try:
        now = subprocess.run(
            ["redis-cli", "-u", url, "TIME"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        seconds = int(now[0])
        window = 15 * 4
        subprocess.run(
            ["redis-cli", "-u", url, "ZADD", key, str(seconds), fresh, str(seconds), dotted],
            capture_output=True,
            text=True,
            check=True,
        )
        subprocess.run(
            ["redis-cli", "-u", url, "ZADD", key, str(seconds - window - 1), stale],
            capture_output=True,
            text=True,
            check=True,
        )

        async def probe():
            client = Client(url, config=Config())

            @actor(queue=queue, max_retries=0)
            async def marker(project_id: str) -> None:
                return None

            await client.send(marker, project_id="live")
            progressed = {"ok": False}

            async def other():
                await asyncio.sleep(0.01)
                progressed["ok"] = True

            task = asyncio.create_task(other())
            saw = False
            alive = None
            for _ in range(40):
                alive = await client.worker_alive(fresh)
                if progressed["ok"]:
                    saw = True
                    break
            await task
            stale_alive = await client.worker_alive(stale)
            unknown_alive = await client.worker_alive(unknown)
            dotted_alive = await client.worker_alive(dotted)
            bad = await client.worker_alive("not-a-worker")
            await client.close()
            return alive, stale_alive, unknown_alive, dotted_alive, bad, saw

        alive, stale_alive, unknown_alive, dotted_alive, bad, saw = asyncio.run(probe())
        assert alive is True
        assert dotted_alive is True
        assert stale_alive is False
        assert unknown_alive is False
        assert bad is False
        assert saw is True
    finally:
        _redis("DEL", key, f"{queue}:data", f"{queue}:pending", f"{queue}:scheduled", db=15)
