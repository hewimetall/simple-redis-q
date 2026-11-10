"""The message the handler is allowed to see. No Redis and no queue client."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any


_CURRENT: contextvars.ContextVar[CurrentMessage | None] = contextvars.ContextVar(
    "simple_q_current_message",
    default=None,
)


@dataclass
class CurrentMessage:
    message_id: str
    retries: int
    max_retries: int
    queue_name: str
    actor_name: str
    kwargs: dict[str, Any]
    headers: dict[str, str]
    deliveries: int = 1
    worker_name: str = ""

    def asdict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "queue_name": self.queue_name,
            "actor_name": self.actor_name,
            "kwargs": self.kwargs,
            "headers": self.headers,
            "deliveries": self.deliveries,
            "worker_name": self.worker_name,
        }


def current_message() -> CurrentMessage | None:
    return _CURRENT.get()


def worker_name() -> str | None:
    """Name of the worker process that is running the current handler.

    Empty outside a handler. The name is the Apalis worker id, so a caller
    can match a stream consumer to this process.
    """

    message = current_message()
    if message is None or not message.worker_name:
        return None
    return message.worker_name


def bind_message(message: CurrentMessage):
    return _CURRENT.set(message)


def reset_message(token) -> None:
    _CURRENT.reset(token)
