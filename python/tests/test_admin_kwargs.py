"""A queued or dead record whose kwargs are not a dict becomes an empty dict."""

from __future__ import annotations

import asyncio

import pytest

from simple_q.admin import QueueAdmin, _dead_page, _mapping, _task_page


def test_given_a_non_dict_kwargs_field_when_a_page_is_built_then_kwargs_are_empty() -> None:
    # Arrange
    dead = {"messages": [{"kwargs": "nope", "reason": "retries"}]}
    waiting = {"tasks": [{"kwargs": 3}], "cursor": "next"}

    # Act
    dead_page = _dead_page(dead)
    task_page = _task_page(waiting)

    # Assert
    assert dead_page.messages[0].kwargs == {}
    assert task_page.tasks[0].kwargs == {}
    assert task_page.cursor == "next"


class _Admin:
    def __init__(self, client: object) -> None:
        self.client = client

    async def snapshot(self, queues: list[str], host: str | None) -> dict:
        return {
            "taken_at": 1,
            "queues": [{"name": "default"}],
            "supervisors": [
                {
                    "host": "local",
                    "pid": 7,
                    "children": [
                        {
                            "pid": 8,
                            "ppid": 7,
                            "tasks": [
                                {
                                    "queue": "default",
                                    "task_id": "t",
                                    "deliveries": 2,
                                }
                            ],
                        }
                    ],
                }
            ],
            "orphans": [],
        }

    async def dead(self, queue: str, limit: int, cursor: str | None) -> dict:
        return {"messages": [], "cursor": cursor, "limit": limit}

    async def peek(self, queue: str, state: str, limit: int, cursor: str | None) -> dict:
        return {"tasks": [], "cursor": cursor, "state": state, "limit": limit}


def test_given_a_client_when_admin_pages_are_read_then_limits_and_state_are_checked(monkeypatch) -> None:
    # Arrange
    monkeypatch.setattr("simple_q.admin.Admin", _Admin)
    admin = QueueAdmin(object())

    async def run() -> tuple:
        snapshot = await admin.snapshot(["default"], "local")
        dead = await admin.dead("default", limit=500, cursor="c")
        waiting = await admin.peek("default", "scheduled", limit=50)
        return snapshot, dead, waiting

    # Act
    snapshot, dead, waiting = asyncio.run(run())

    # Assert
    assert snapshot.supervisors[0].children[0].ppid == 7
    assert snapshot.supervisors[0].children[0].tasks[0].deliveries == 2
    assert dead.cursor == "c"
    assert waiting.cursor is None
    assert _mapping({"taken_at": 1}) == {"taken_at": 1}
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(admin.dead("default", limit=0))
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(admin.dead("default", limit=True))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="state"):
        asyncio.run(admin.peek("default", "done"))
