import asyncio
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

release_calls = {"count": 0, "error": None}

runtime = types.ModuleType("simple_q_runtime")


class RuntimeController:
    pass


async def acquire_lock(key, ttl):
    return "owner-1"


async def release_lock(key, owner):
    release_calls["count"] += 1
    error = release_calls["error"]
    if error is not None:
        raise error
    return True


async def enqueue(queue, task, payload):
    return None


runtime.Controller = RuntimeController
runtime.acquire_lock = acquire_lock
runtime.release_lock = release_lock
runtime.enqueue = enqueue
sys.modules["simple_q_runtime"] = runtime

from simple_q.controller import Controller, task


class App(Controller):
    queue = "default"

    @task(
        entrypoint=False,
        lock_key="cache:discovery",
        lock_ttl=30,
        skip_on_lock=False,
        max_retries=0,
        next=None,
        err=None,
    )
    async def discovery(self, value: str):
        if value == "boom":
            raise RuntimeError("boom")
        return {"ok": True, "value": value}


def test_release_error_keeps_a_successful_result():
    release_calls["count"] = 0
    release_calls["error"] = RuntimeError("redis down")
    result = asyncio.run(App().execute("discovery", {"value": "done"}))
    assert result == {"ok": True, "value": "done"}
    assert release_calls["count"] == 1


def test_release_error_does_not_hide_the_handler_error():
    release_calls["count"] = 0
    release_calls["error"] = RuntimeError("redis down")
    try:
        asyncio.run(App().execute("discovery", {"value": "boom"}))
    except RuntimeError as error:
        assert str(error) == "boom"
    else:
        raise AssertionError("handler error was swallowed")
    assert release_calls["count"] == 1
