"""Dispatch branches that reject a message or take the non-awaitable hook path."""

from __future__ import annotations

import asyncio

import pytest

from simple_q import App, actor
from simple_q.dispatch import (
    _delivery_count,
    build_message,
    resolve_lock_key,
    usable_lock_key,
)


def _message(**overrides: object) -> dict:
    message = build_message(
        actor_name="gated",
        queue="default",
        kwargs={},
        headers={},
        max_retries=0,
        lock_key="demo:gate",
        message_id="edge-1",
    )
    message.update(overrides)
    return message


def test_given_actor_options_when_the_decorator_is_applied_then_the_function_name_is_kept() -> None:
    # Arrange
    decorate = actor(queue="default", max_retries=0)

    # Act
    @decorate
    async def named() -> int:
        return 1

    @actor
    async def bare() -> int:
        return 2

    # Assert
    assert named.name == "named"
    assert named.queue == "default"
    assert asyncio.run(named.function()) == 1
    assert bare.name == "bare"
    assert bare.queue == "default"
    assert asyncio.run(bare.function()) == 2


def test_given_invalid_message_fields_when_build_message_runs_then_each_field_is_rejected() -> None:
    # Arrange
    common = {"actor_name": "recalc", "queue": "default", "max_retries": 0}

    # Act
    kwargs_error = pytest.raises(TypeError, match="kwargs")
    headers_error = pytest.raises(TypeError, match="headers")
    unique_type = pytest.raises(TypeError, match="unique_key")
    unique_empty = pytest.raises(ValueError, match="unique_key")

    # Assert
    with kwargs_error:
        build_message(kwargs=["nope"], headers={}, **common)  # type: ignore[arg-type]
    with headers_error:
        build_message(kwargs={}, headers={"n": 1}, **common)  # type: ignore[dict-item]
    with unique_type:
        build_message(kwargs={}, headers={}, unique_key=1, **common)  # type: ignore[arg-type]
    with unique_empty:
        build_message(kwargs={}, headers={}, unique_key="", **common)


def test_given_a_bad_delivery_count_when_it_is_read_then_the_count_is_one() -> None:
    # Arrange
    samples = ({"deliveries": "nope"}, {"deliveries": ["x"]}, {"deliveries": 0}, {"deliveries": -3})

    # Act
    counts = [_delivery_count(message) for message in samples]

    # Assert
    assert counts == [1, 1, 1, 1]


def test_given_a_busy_lock_without_skip_when_dispatch_runs_then_it_raises() -> None:
    # Arrange
    class Busy:
        async def acquire_lock(self, key: str, ttl: int) -> None:
            return None

        async def release_lock(self, key: str, owner: str) -> bool:
            return True

    @actor(name="gated", queue="default", lock_key="demo:gate", skip_on_lock=False)
    async def gated() -> int:
        return 1

    app = App(client=Busy(), actors=[gated])

    # Act
    with pytest.raises(RuntimeError, match="lock busy: demo:gate") as caught:
        asyncio.run(app.dispatch(_message()))

    # Assert
    assert "demo:gate" in str(caught.value)


def test_given_an_async_dead_hook_when_a_delivery_is_poison_then_the_hook_is_awaited() -> None:
    # Arrange
    seen: list[str] = []

    async def remember(letter) -> None:
        seen.append(letter.reason)

    @actor(name="gated", queue="default", max_retries=0, max_deliveries=1)
    async def gated() -> int:
        raise AssertionError("handler ran")

    app = App(client=object(), actors=[gated], on_dead=[remember])
    message = _message(deliveries=3, max_deliveries=1)

    # Act
    result = asyncio.run(app.dispatch(message))

    # Assert
    assert result["outcome"] == "dead"
    assert result["reason"] == "poison"
    assert seen == ["poison"]


def test_given_a_zero_lock_ttl_on_the_message_when_dispatch_runs_then_it_is_rejected() -> None:
    # Arrange
    class Client:
        async def acquire_lock(self, key: str, ttl: int) -> str:
            raise AssertionError("acquire")

        async def release_lock(self, key: str, owner: str) -> bool:
            return True

    @actor(name="gated", queue="default", lock_key="demo:gate")
    async def gated() -> int:
        return 1

    app = App(client=Client(), actors=[gated])

    # Act
    def dispatch() -> None:
        asyncio.run(app.dispatch(_message(lock_ttl=0)))

    # Assert
    with pytest.raises(ValueError, match="lock_ttl must be greater than zero"):
        dispatch()


def test_given_a_non_string_placeholder_when_the_lock_key_is_filled_then_it_is_stringified() -> None:
    # Arrange
    template = "demo:item:{name}"

    # Act
    resolved = resolve_lock_key(template, {"name": 7})

    # Assert
    assert resolved == "demo:item:7"
    assert usable_lock_key("") is False
    assert usable_lock_key("lock:demo") is False


def test_given_a_sync_around_hook_when_dispatch_runs_then_its_dict_is_the_result() -> None:
    # Arrange
    def around(message: dict, call_next) -> dict:
        return {"outcome": "ok", "result": {"sync": True}}

    @actor(name="gated", queue="default", max_retries=0)
    async def gated() -> int:
        raise AssertionError("handler ran")

    app = App(client=object(), actors=[gated], around_dispatch=[around])

    # Act
    result = asyncio.run(app.dispatch(_message(lock_key=None)))

    # Assert
    assert result == {"outcome": "ok", "result": {"sync": True}}
