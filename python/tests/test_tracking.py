"""``tracked`` names the current task and still returns the result."""

from __future__ import annotations

import asyncio

from simple_q.tracking import tracked


def test_given_a_running_task_when_tracked_awaits_then_the_task_name_is_set() -> None:
    # Arrange
    async def value() -> str:
        task = asyncio.current_task()
        assert task is not None
        return task.get_name()

    # Act
    name = asyncio.run(tracked(value(), "gate"))

    # Assert
    assert name == "gate"


def test_given_no_current_task_when_tracked_awaits_then_the_result_is_returned() -> None:
    # Arrange
    async def value() -> int:
        return 7

    async def run() -> int:
        import simple_q.tracking as tracking

        original = tracking.asyncio.current_task
        tracking.asyncio.current_task = lambda: None
        try:
            return await tracking.tracked(value(), "unused")
        finally:
            tracking.asyncio.current_task = original

    # Act
    result = asyncio.run(run())

    # Assert
    assert result == 7
