"""Pass a custom option on send, and read it from middleware hooks.

``alter`` is an argument of ``recalc``. ``send`` stores it in ``kwargs``.
The middleware copies it into the ``alter`` header, because headers are
strings. ``note`` is a header the handler does not take as an argument.
``lane`` is the same for every send of this actor, so it lives in
``ACTOR_LANE`` in this module. The worker imports the module. ``on_send``
copies the lane onto the message.

Run from the repository root:

    PYTHONPATH=python python python/examples/alter_option.py
"""

from __future__ import annotations

import asyncio

from simple_q import App, MemoryClient, actor
from simple_q.message import current_message

# @actor accepts only its own parameters. A flag shared by every send of
# one actor lives next to the declaration.
ACTOR_LANE = {"recalc": "batch"}


class AlterMiddleware:
    """Record each delivery and copy ``alter`` into a string header."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.dead: list = []
        self.seen: list[dict] = []

    def install(self, client, app) -> None:
        client.on_send(self.on_send)
        app.around_dispatch(self.around)
        app.on_start(self.on_start)
        app.on_stop(self.on_stop)
        app.on_dead(self.on_dead)

    def on_send(self, actor_name, kwargs, headers):
        updated = dict(headers)
        updated["alter"] = "1" if kwargs.get("alter") is True else "0"
        lane = ACTOR_LANE.get(actor_name)
        if lane is not None:
            updated["lane"] = lane
        return updated

    async def around(self, message, call_next):
        outcome = await call_next()
        headers = message.get("headers") or {}
        kwargs = message.get("kwargs") or {}
        self.seen.append(
            {
                "project_id": kwargs.get("project_id"),
                "kwargs_alter": kwargs.get("alter"),
                "header_alter": headers.get("alter"),
                "lane": headers.get("lane"),
                "note": headers.get("note"),
                "outcome": outcome.get("outcome"),
                "result": outcome.get("result"),
            }
        )
        return outcome

    async def on_start(self) -> None:
        self.started = True

    async def on_stop(self) -> None:
        self.stopped = True

    async def on_dead(self, letter) -> None:
        self.dead.append(letter)


@actor(queue="default", max_retries=0)
async def recalc(project_id: str, alter: bool = False) -> str:
    message = current_message()
    if message is None or message.kwargs["alter"] is not alter:
        raise RuntimeError("alter argument did not reach the handler")
    expected = "1" if alter else "0"
    if message.headers.get("alter") != expected:
        raise RuntimeError("alter header does not match the argument")
    if message.headers.get("lane") != ACTOR_LANE["recalc"]:
        raise RuntimeError("lane header was not copied from the actor table")
    if alter:
        return "altered"
    return str(message.headers.get("note") or "")


def create_app(client, middleware: AlterMiddleware | None = None) -> App:
    middleware = middleware or AlterMiddleware()
    app = App(client=client, actors=[recalc])
    middleware.install(client, app)
    return app


async def run() -> AlterMiddleware:
    client = MemoryClient()
    middleware = AlterMiddleware()
    app = create_app(client, middleware)
    await client.send(recalc, project_id="42", alter=True)
    await client.send_with_options(
        recalc,
        kwargs={"project_id": "7", "alter": False},
        headers={"note": "from-caller"},
    )
    await client.drain(app)
    return middleware


EXPECTED = [
    {
        "project_id": "42",
        "kwargs_alter": True,
        "header_alter": "1",
        "lane": "batch",
        "note": None,
        "outcome": "ok",
        "result": "altered",
    },
    {
        "project_id": "7",
        "kwargs_alter": False,
        "header_alter": "0",
        "lane": "batch",
        "note": "from-caller",
        "outcome": "ok",
        "result": "from-caller",
    },
]


def main() -> None:
    middleware = asyncio.run(run())
    if not middleware.started or not middleware.stopped:
        raise SystemExit("start and stop hooks did not run")
    if middleware.dead:
        raise SystemExit(f"unexpected dead letters: {middleware.dead}")
    if middleware.seen != EXPECTED:
        raise SystemExit(f"unexpected deliveries: {middleware.seen}")
    print("alter option example ok")


if __name__ == "__main__":
    main()
