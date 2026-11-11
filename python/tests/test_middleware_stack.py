"""Four middleware classes share one App. Install order is the stack order."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q import App, MemoryClient, actor
from simple_q.dispatch import build_message
from simple_q.message import current_message


class Layer:
    def __init__(self, name: str, trace: list, *, skip: bool = False, fail_start: bool = False, fail_dead: bool = False):
        self.name = name
        self.trace = trace
        self.skip = skip
        self.fail_start = fail_start
        self.fail_dead = fail_dead

    def install(self, client, app) -> None:
        client.on_send(self.on_send)
        app.around_dispatch(self.around)
        app.on_start(self.on_start)
        app.on_stop(self.on_stop)
        app.on_dead(self.on_dead)

    def on_send(self, actor_name, kwargs, headers):
        self.trace.append(("send", self.name, headers.get("last")))
        updated = dict(headers)
        updated[self.name] = self.name
        updated["last"] = self.name
        return updated

    async def around(self, message, call_next):
        self.trace.append(("enter", self.name, current_message().actor_name))
        if self.skip:
            self.trace.append(("skip", self.name))
            return {"outcome": "skipped"}
        outcome = await call_next()
        if self.name == "c" and outcome.get("outcome") == "retry":
            outcome = dict(outcome)
            outcome["delay_ms"] = 5
        self.trace.append(("exit", self.name, outcome.get("outcome"), outcome.get("delay_ms")))
        return outcome

    async def on_start(self) -> None:
        self.trace.append(("start", self.name))
        if self.fail_start:
            raise RuntimeError("start failed")

    async def on_stop(self) -> None:
        self.trace.append(("stop", self.name))

    def on_dead(self, letter) -> None:
        self.trace.append(("dead", self.name, letter.reason, letter.kwargs.get("alter")))
        if self.fail_dead:
            raise RuntimeError("dead failed")


def _install(client, app, layers: list[Layer]) -> None:
    for layer in layers:
        layer.install(client, app)


def test_four_middlewares_wrap_in_install_order_and_pass_headers():
    trace: list = []
    client = MemoryClient()

    @actor(queue="default", max_retries=0)
    async def work(value: str):
        message = current_message()
        trace.append(("handler", value, message.headers.get("last"), message.headers.get("a")))
        return value

    app = App(client=client, actors=[work])
    layers = [Layer(name, trace) for name in ("a", "b", "c", "d")]
    _install(client, app, layers)

    asyncio.run(client.send(work, value="one"))
    asyncio.run(client.drain(app))

    assert trace == [
        ("send", "a", None),
        ("send", "b", "a"),
        ("send", "c", "b"),
        ("send", "d", "c"),
        ("start", "a"),
        ("start", "b"),
        ("start", "c"),
        ("start", "d"),
        ("enter", "a", "work"),
        ("enter", "b", "work"),
        ("enter", "c", "work"),
        ("enter", "d", "work"),
        ("handler", "one", "d", "a"),
        ("exit", "d", "ok", None),
        ("exit", "c", "ok", None),
        ("exit", "b", "ok", None),
        ("exit", "a", "ok", None),
        ("stop", "a"),
        ("stop", "b"),
        ("stop", "c"),
        ("stop", "d"),
    ]


def test_outer_middleware_can_skip_the_rest_of_the_stack():
    trace: list = []
    client = MemoryClient()

    @actor(queue="default", max_retries=0)
    async def work():
        trace.append("handler")

    app = App(client=client, actors=[work])
    _install(
        client,
        app,
        [
            Layer("a", trace, skip=True),
            Layer("b", trace),
            Layer("c", trace),
            Layer("d", trace),
        ],
    )
    message = build_message(
        actor_name="work",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        message_id="skip-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "skipped"}
    assert trace == [("enter", "a", "work"), ("skip", "a")]


def test_inner_retry_change_is_visible_to_every_outer_layer():
    trace: list = []
    client = MemoryClient()

    @actor(queue="default", max_retries=1)
    async def work():
        trace.append("handler")
        raise RuntimeError("nope")

    app = App(client=client, actors=[work])
    _install(client, app, [Layer(name, trace) for name in ("a", "b", "c", "d")])
    message = build_message(
        actor_name="work",
        queue="default",
        kwargs={},
        headers={},
        max_retries=1,
        message_id="retry-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "retry", "delay_ms": 5}
    exits = [item for item in trace if isinstance(item, tuple) and item[0] == "exit"]
    assert [item[:3] for item in exits] == [
        ("exit", "d", "retry"),
        ("exit", "c", "retry"),
        ("exit", "b", "retry"),
        ("exit", "a", "retry"),
    ]
    # d sees the backoff from dispatch. c replaces it, and b and a see 5.
    assert isinstance(exits[0][3], int) and exits[0][3] != 5
    assert [item[3] for item in exits[1:]] == [5, 5, 5]
    assert not any(isinstance(item, tuple) and item[0] == "dead" for item in trace)


def test_dead_hooks_run_in_install_order_when_one_of_them_raises():
    trace: list = []
    client = MemoryClient()

    @actor(queue="default", max_retries=0)
    async def work(alter: bool = False):
        trace.append("handler")
        raise RuntimeError("nope")

    app = App(client=client, actors=[work])
    _install(
        client,
        app,
        [
            Layer("a", trace),
            Layer("b", trace, fail_dead=True),
            Layer("c", trace),
            Layer("d", trace),
        ],
    )
    message = build_message(
        actor_name="work",
        queue="default",
        kwargs={"alter": True},
        headers={},
        max_retries=0,
        message_id="dead-1",
    )
    result = asyncio.run(app.dispatch(message))
    assert result == {"outcome": "dead", "reason": "retries"}
    assert trace == [
        ("enter", "a", "work"),
        ("enter", "b", "work"),
        ("enter", "c", "work"),
        ("enter", "d", "work"),
        "handler",
        ("dead", "a", "retries", True),
        ("dead", "b", "retries", True),
        ("dead", "c", "retries", True),
        ("dead", "d", "retries", True),
        ("exit", "d", "dead", None),
        ("exit", "c", "dead", None),
        ("exit", "b", "dead", None),
        ("exit", "a", "dead", None),
    ]


def test_a_failed_start_hook_does_not_stop_the_later_ones():
    trace: list = []
    client = MemoryClient()

    @actor(queue="default", max_retries=0)
    async def work():
        return None

    app = App(client=client, actors=[work])
    _install(
        client,
        app,
        [
            Layer("a", trace),
            Layer("b", trace, fail_start=True),
            Layer("c", trace),
            Layer("d", trace),
        ],
    )
    asyncio.run(app.startup())
    assert trace == [("start", "a"), ("start", "b"), ("start", "c"), ("start", "d")]
