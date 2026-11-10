"""In-memory stand-in for Redis enqueue.

``enable_memory`` makes ``Actor.send`` append messages to a process-local
queue. ``drain`` runs those messages through ``Registry.dispatch`` until the
queue is empty, including messages sent while a handler is running.
"""

from __future__ import annotations

_ENABLED = False
_QUEUES: dict[str, list[dict]] = {}


def enable_memory() -> None:
    global _ENABLED
    _ENABLED = True
    _QUEUES.clear()


def disable_memory() -> None:
    global _ENABLED
    _ENABLED = False
    _QUEUES.clear()


def enabled() -> bool:
    return _ENABLED


def push(message: dict) -> None:
    _QUEUES.setdefault(str(message["queue"]), []).append(message)


def pop(queue: str) -> dict | None:
    pending = _QUEUES.get(queue)
    if not pending:
        return None
    return pending.pop(0)


async def drain(queue: str, registry) -> None:
    while True:
        message = pop(queue)
        if message is None:
            return
        await registry.dispatch(message)
