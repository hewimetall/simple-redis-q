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
    assert result == {"outcome": "dead", "reason": "retries", "error": "RuntimeError: boom"}


def test_lock_ttl_rejects_zero():
    try:

        @actor(queue="default", lock_ttl=0)
        async def nope():
            return None

        raise AssertionError("zero lock_ttl was accepted")
    except ValueError as error:
        assert "lock_ttl" in str(error)
    try:
        build_message(
            actor_name="discovery",
            queue="default",
            kwargs={},
            headers={},
            max_retries=0,
            lock_ttl=0,
        )
    except ValueError as error:
        assert "lock_ttl" in str(error)
    else:
        raise AssertionError("zero lock_ttl was accepted")


def test_cancel_during_release_still_unlocks():
    class Client:
        def __init__(self):
            self.released = False
            self.in_release = asyncio.Event()

        async def acquire_lock(self, key, ttl):
            return "owner-1"

        async def release_lock(self, key, owner):
            self.in_release.set()
            await asyncio.sleep(0.05)
            self.released = True
            return True

    local = Client()

    @actor(queue="default", lock_key="cache:discovery", lock_ttl=30, max_retries=0)
    async def work():
        return "ok"

    async def main():
        app = App(client=local, actors=[work])
        message = build_message(
            actor_name="work",
            queue="default",
            kwargs={},
            headers={},
            max_retries=0,
            lock_key="cache:discovery",
            message_id="cancel-release",
        )
        task = asyncio.create_task(app.dispatch(message))
        await asyncio.wait_for(local.in_release.wait(), timeout=1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert local.released

    asyncio.run(main())
