"""Runtime checks for the client contract and the admin dict boundary."""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q import MemoryClient
from simple_q.admin import _dead_page, _snapshot, _task_page


def test_memory_lock_accepts_ttl_seconds_and_ttl() -> None:
    async def run() -> None:
        client = MemoryClient()
        assert await client.acquire_lock("a", 5) == "memory"
        assert await client.release_lock("a", "memory") is True
        assert await client.acquire_lock("b", ttl_seconds=5) == "memory"
        assert await client.release_lock("b", "memory") is True
        assert await client.acquire_lock("c", ttl=5) == "memory"
        with pytest.raises(TypeError, match="ttl_seconds"):
            await client.acquire_lock("d")
        with pytest.raises(TypeError, match="both"):
            await client.acquire_lock("e", 1, ttl=2)

    asyncio.run(run())


def test_native_lock_rejects_a_missing_or_conflicting_ttl() -> None:
    from simple_q import Client, Config

    client = Client("redis://127.0.0.1:1/0", Config())
    with pytest.raises(TypeError, match="ttl_seconds"):
        client.acquire_lock("gate")
    with pytest.raises(TypeError, match="both"):
        client.acquire_lock("gate", 1, ttl=2)


def test_on_send_keyword_is_fn_on_both_clients() -> None:
    from simple_q import Client, Config

    def hook(actor_name: str, kwargs: dict, headers: dict) -> dict:
        return headers

    memory = MemoryClient()
    assert memory.on_send(fn=hook) is hook
    native = Client("redis://127.0.0.1:1/0", Config())
    assert native.on_send(fn=hook) is hook


def test_admin_pages_keep_tuples_and_nested_lists() -> None:
    page = _dead_page(
        {
            "messages": [
                {
                    "task_id": "t",
                    "kwargs": {"n": [1, 2]},
                    "reason": "retries",
                }
            ],
            "cursor": None,
        }
    )
    assert isinstance(page.messages, tuple)
    assert page.messages[0].kwargs["n"] == [1, 2]

    waiting = _task_page(
        {
            "tasks": [{"task_id": "t", "kwargs": {"flags": [True, False]}}],
            "cursor": "next",
        }
    )
    assert isinstance(waiting.tasks, tuple)
    assert waiting.tasks[0].kwargs["flags"] == [True, False]

    snapshot = _snapshot(
        {
            "taken_at": 1,
            "queues": [{"name": "default"}],
            "supervisors": [
                {
                    "host": "local",
                    "pid": 7,
                    "queues": ["default"],
                    "children": [
                        {
                            "pid": 8,
                            "workers": ["agent-worker.default.local.8.a"],
                            "tasks": [],
                        }
                    ],
                }
            ],
            "orphans": [],
        }
    )
    assert snapshot.supervisors[0].queues == ["default"]
    assert isinstance(snapshot.supervisors[0].children[0].workers, list)
    assert snapshot.supervisors[0].children[0].tasks == []


def test_admin_result_must_be_a_dict() -> None:
    from simple_q.admin import _mapping

    with pytest.raises(TypeError, match="dict"):
        _mapping("{\"taken_at\": 1}")
