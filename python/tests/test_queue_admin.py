"""``QueueAdmin.dead`` and ``QueueAdmin.peek`` page Redis one slice at a time."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import json
import os
import subprocess
import time

import pytest

from simple_q.admin import DeadPage, QueueAdmin, TaskPage


def _redis(*args: str) -> str:
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
    result = subprocess.run(
        ["redis-cli", "-u", url, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    return result.stdout.strip()


def _redis_up() -> bool:
    try:
        return _redis("PING") == "PONG"
    except (AssertionError, FileNotFoundError):
        return False


def _queue(prefix: str) -> str:
    return f"{prefix}{os.getpid():x}{time.time_ns() & 0xFFFFFFFF:08x}"


def _job(queue: str, message_id: str) -> str:
    return json.dumps(
        {
            "version": 1,
            "message_id": message_id,
            "actor_name": "recalc",
            "queue": queue,
            "kwargs": {"project": "42"},
            "headers": {},
            "retries": 1,
            "max_retries": 2,
            "time_limit_ms": 1_800_000,
            "min_backoff_ms": 15_000,
            "max_backoff_ms": 86_400_000,
            "max_deliveries": 5,
            "lock_ttl": 30,
            "skip_on_lock": False,
        }
    )


def _admin() -> QueueAdmin:
    from simple_q import Client, Config

    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
    return QueueAdmin(Client(url, config=Config()))


def test_limit_zero_and_an_unknown_state_are_rejected():
    admin = _admin()
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(admin.dead("default", limit=0))
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(admin.peek("default", "active", limit=0))
    with pytest.raises(ValueError, match="state"):
        asyncio.run(admin.peek("default", "done"))


def test_dead_pages_newest_first_without_the_apalis_set():
    if not _redis_up():
        pytest.skip("redis is not reachable")
    queue = _queue("dead")
    data = f"{queue}:data"
    index = f"simple_q:dead:{queue}"
    apalis = f"{queue}:dead"
    keys = [data, index, apalis]
    for key in keys:
        _redis("DEL", key)
    try:
        rows = (
            ("task-d", "40", "retries", "RuntimeError: boom", "1"),
            ("task-c", "30", "timeout", "TimeoutError: slow", "2"),
            ("task-b", "30", "poison", "", "4"),
            ("task-a", "20", "invalid", "", "0"),
        )
        for task_id, score, reason, error, deliveries in rows:
            meta = f"{queue}:meta:{task_id}"
            keys.append(meta)
            _redis("HSET", data, task_id, _job(queue, f"msg-{task_id}"))
            _redis("ZADD", index, score, task_id)
            _redis(
                "HSET",
                meta,
                "reason",
                reason,
                "dead_at",
                score,
                "deliveries",
                deliveries,
            )
            if error:
                _redis("HSET", meta, "error", error)
        assert _redis("SCARD", apalis) == "0"

        admin = _admin()
        first = asyncio.run(admin.dead(queue, limit=2))
        assert isinstance(first, DeadPage)
        assert [item.task_id for item in first.messages] == ["task-d", "task-c"]
        assert first.messages[0].message_id == "msg-task-d"
        assert first.messages[0].actor_name == "recalc"
        assert first.messages[0].kwargs == {"project": "42"}
        assert first.messages[0].reason == "retries"
        assert first.messages[0].error == "RuntimeError: boom"
        assert first.messages[0].retries == 1
        assert first.messages[0].deliveries == 1
        assert first.messages[0].dead_at == 40
        assert first.messages[1].error == "TimeoutError: slow"
        assert first.cursor

        second = asyncio.run(admin.dead(queue, limit=10_000, cursor=first.cursor))
        assert [item.task_id for item in second.messages] == ["task-b", "task-a"]
        assert second.messages[0].reason == "poison"
        assert second.messages[0].error is None
        assert second.messages[0].deliveries == 4
        assert second.cursor is None
        with pytest.raises(dataclasses.FrozenInstanceError):
            first.cursor = None  # type: ignore[misc]
    finally:
        for key in keys:
            _redis("DEL", key)


def test_peek_pages_active_and_scheduled():
    if not _redis_up():
        pytest.skip("redis is not reachable")
    queue = _queue("peek")
    active = f"{queue}:active"
    scheduled = f"{queue}:scheduled"
    data = f"{queue}:data"
    keys = [active, scheduled, data]
    for key in keys:
        _redis("DEL", key)
    try:
        for task_id in ("act-1", "act-2", "act-3", "not-listed"):
            _redis("HSET", data, task_id, _job(queue, f"msg-{task_id}"))
        _redis("RPUSH", active, "act-1", "act-2", "act-3")
        admin = _admin()
        waiting = asyncio.run(admin.peek(queue, "active", limit=2))
        assert isinstance(waiting, TaskPage)
        assert [task.task_id for task in waiting.tasks] == ["act-1", "act-2"]
        assert waiting.tasks[0].message_id == "msg-act-1"
        assert waiting.tasks[0].kwargs == {"project": "42"}
        assert waiting.tasks[0].run_at is None
        assert "not-listed" not in {task.task_id for task in waiting.tasks}
        rest = asyncio.run(admin.peek(queue, "active", cursor=waiting.cursor))
        assert [task.task_id for task in rest.tasks] == ["act-3"]
        assert rest.cursor is None

        stale = base64.urlsafe_b64encode(b"a\nact-2").decode().rstrip("=")
        with pytest.raises(ValueError):
            asyncio.run(admin.peek(queue, "active", cursor=stale))
        with pytest.raises(ValueError):
            asyncio.run(admin.peek(queue, "active", cursor="%%%"))

        _redis("DEL", active)
        _redis("RPUSH", active, "act-1", "act-2", "act-3")
        first = asyncio.run(admin.peek(queue, "active", limit=2))
        _redis("LREM", active, "1", "act-2")
        shifted = asyncio.run(admin.peek(queue, "active", cursor=first.cursor))
        assert shifted.tasks == ()

        _redis("ZADD", scheduled, "100", "sch-a", "200", "sch-c", "200", "sch-b")
        for task_id in ("sch-a", "sch-b", "sch-c"):
            _redis("HSET", data, task_id, _job(queue, f"msg-{task_id}"))
        soon = asyncio.run(admin.peek(queue, "scheduled", limit=2))
        assert [(task.task_id, task.run_at) for task in soon.tasks] == [
            ("sch-a", 100),
            ("sch-b", 200),
        ]
        later = asyncio.run(admin.peek(queue, "scheduled", cursor=soon.cursor))
        assert [(task.task_id, task.run_at) for task in later.tasks] == [("sch-c", 200)]
        assert later.cursor is None
    finally:
        for key in keys:
            _redis("DEL", key)
