"""Memory client branches for hooks, a closed client, locks, and a failed drain."""

from __future__ import annotations

import asyncio

import pytest

from simple_q import MemoryClient, actor


def _actor():
    @actor(name="recalc", queue="default", max_retries=0)
    async def recalc(n: int = 1) -> int:
        return n

    return recalc


def test_given_an_on_send_hook_returns_none_when_sending_then_later_headers_stay() -> None:
    # Arrange
    client = MemoryClient()
    work = _actor()

    def skip(actor_name: str, kwargs: dict, headers: dict) -> None:
        return None

    def stamp(actor_name: str, kwargs: dict, headers: dict) -> dict:
        return {"traceparent": "00-ab"}

    client.on_send(skip)
    client.on_send(stamp)

    # Act
    asyncio.run(client.send(work, n=1))

    # Assert
    assert client._queues["default"][0]["headers"]["traceparent"] == "00-ab"


def test_given_an_on_send_hook_returns_a_list_when_sending_then_it_is_rejected() -> None:
    # Arrange
    client = MemoryClient()
    work = _actor()

    def bad(actor_name: str, kwargs: dict, headers: dict) -> list:
        return ["nope"]

    client.on_send(bad)

    # Act
    with pytest.raises(TypeError, match="on_send hook must return a headers dict"):
        asyncio.run(client.send(work, n=1))

    # Assert
    assert client.depth("default") == 0


def test_given_send_with_options_returns_none_when_send_runs_then_it_raises() -> None:
    # Arrange
    client = MemoryClient()
    work = _actor()

    async def blocked(*args, **kwargs):
        return None

    client.send_with_options = blocked  # type: ignore[method-assign]

    # Act
    with pytest.raises(RuntimeError, match="did not enqueue"):
        asyncio.run(client.send(work, n=1))

    # Assert
    assert client.depth("default") == 0


def test_given_a_closed_client_when_send_with_options_runs_then_it_raises() -> None:
    # Arrange
    client = MemoryClient()
    work = _actor()

    # Act
    asyncio.run(client.close())

    # Assert
    with pytest.raises(RuntimeError, match="client is closed"):
        asyncio.run(client.send_with_options(work, kwargs={"n": 1}))


def test_given_a_delay_when_send_with_options_runs_then_delay_ms_is_stored() -> None:
    # Arrange
    client = MemoryClient()
    work = _actor()

    # Act
    asyncio.run(client.send_with_options(work, kwargs={"n": 1}, delay=25))

    # Assert
    assert client._queues["default"][0]["delay_ms"] == 25


def test_given_a_held_lock_when_acquire_and_release_run_then_the_owner_is_checked() -> None:
    # Arrange
    client = MemoryClient()

    async def run() -> tuple:
        first = await client.acquire_lock("demo:gate", 30)
        second = await client.acquire_lock("demo:gate", ttl=30)
        wrong = await client.release_lock("demo:gate", "other")
        released = await client.release_lock("demo:gate", "memory")
        return first, second, wrong, released

    # Act
    first, second, wrong, released = asyncio.run(run())

    # Assert
    assert first == "memory"
    assert second is None
    assert wrong is False
    assert released is True


def test_given_dispatch_raises_when_drain_runs_then_the_unique_key_is_released() -> None:
    # Arrange
    client = MemoryClient()
    work = _actor()

    class Exploding:
        async def startup(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

        async def dispatch(self, message: dict) -> dict:
            raise RuntimeError("dispatch failed")

    async def run() -> str | None:
        first = await client.send_with_options(work, kwargs={"n": 1}, unique_key="once")
        assert first is not None
        with pytest.raises(RuntimeError, match="dispatch failed"):
            await client.drain(Exploding())
        return await client.send_with_options(work, kwargs={"n": 1}, unique_key="once")

    # Act
    second = asyncio.run(run())

    # Assert
    assert second is not None
    assert client.depth("default") == 1
