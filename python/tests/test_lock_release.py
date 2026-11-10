import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q import App, actor
from simple_q.dispatch import build_message


class FakeClient:
    def __init__(self):
        self.release_error = None

    async def acquire_lock(self, key, ttl):
        return "owner-1"

    async def release_lock(self, key, owner):
        error = self.release_error
        if error is not None:
            raise error
        return True


client = FakeClient()


@actor(
    queue="default",
    lock_key="cache:discovery",
    lock_ttl=30,
    skip_on_lock=False,
    max_retries=0,
)
async def discovery(value: str):
    if value == "boom":
        raise RuntimeError("boom")
    return {"ok": True, "value": value}


def _app():
    return App(client=client, actors=[discovery])


def _message(value: str) -> dict:
    return build_message(
        actor_name="discovery",
        queue="default",
        kwargs={"value": value},
        headers={},
        max_retries=0,
        lock_key="cache:discovery",
        message_id=f"lock-{value}",
    )


def test_release_error_keeps_a_successful_result():
    client.release_error = RuntimeError("redis down")
    result = asyncio.run(_app().dispatch(_message("done")))
    assert result["outcome"] == "ok"
    assert result["result"] == {"ok": True, "value": "done"}


def test_release_error_does_not_hide_the_handler_error():
    client.release_error = RuntimeError("redis down")
    result = asyncio.run(_app().dispatch(_message("boom")))
    assert result == {"outcome": "dead", "reason": "retries"}
