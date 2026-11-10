"""In-memory stand-in for Redis enqueue.

``enable_memory`` makes ``Actor.send`` append messages to a process-local
queue. ``drain`` runs those messages through ``Registry.dispatch`` until the
queue is empty, including messages sent while a handler is running.

A ``unique_key`` is held while its message is queued or being drained.
A second send of that key returns no message id until ``drain`` finishes it.
This mode does not open Redis.
"""

from __future__ import annotations

_ENABLED = False
_QUEUES: dict[str, list[dict]] = {}
_HELD: set[tuple[str, str]] = set()


def enable_memory() -> None:
    global _ENABLED
    _ENABLED = True
    _QUEUES.clear()
    _HELD.clear()


def disable_memory() -> None:
    global _ENABLED
    _ENABLED = False
    _QUEUES.clear()
    _HELD.clear()


def enabled() -> bool:
    return _ENABLED


def depth(queue: str) -> int:
    return len(_QUEUES.get(str(queue), ()))


def _token(message: dict) -> tuple[str, str] | None:
    key = message.get("unique_key")
    if not isinstance(key, str) or not key:
        return None
    return (str(message.get("queue")), key)


def push(message: dict) -> bool:
    token = _token(message)
    if token is not None and token in _HELD:
        return False
    if token is not None:
        _HELD.add(token)
    _QUEUES.setdefault(str(message["queue"]), []).append(message)
    return True


def pop(queue: str) -> dict | None:
    pending = _QUEUES.get(queue)
    if not pending:
        return None
    return pending.pop(0)


def _release(message: dict) -> None:
    token = _token(message)
    if token is not None:
        _HELD.discard(token)


async def drain(queue: str, registry) -> None:
    while True:
        message = pop(queue)
        if message is None:
            return
        try:
            await registry.dispatch(message)
        finally:
            _release(message)
