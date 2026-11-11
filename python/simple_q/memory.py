"""In-memory client with the same awaitable interface as ``Client``.

``drain`` runs queued messages through ``App.dispatch``, one at a time,
including messages sent by a handler. It calls ``startup`` and ``shutdown``.
A ``retry`` outcome goes back on the queue immediately, without waiting out
the backoff, and its ``unique_key`` stays held. A terminal outcome releases
the key. This client does not open Redis.
"""

from __future__ import annotations

from simple_q.dispatch import build_message


class MemoryClient:
    def __init__(self, config=None):
        self.config = config
        self._queues: dict[str, list[dict]] = {}
        self._held: set[tuple[str, str]] = set()
        self._on_send = []
        self._locks: dict[str, str] = {}
        self._alive: set[str] = set()
        self._closed = False

    def on_send(self, fn):
        self._on_send.append(fn)
        return fn

    def _apply_on_send(self, actor_name: str, kwargs: dict, headers: dict) -> dict:
        current = dict(headers)
        for hook in self._on_send:
            updated = hook(actor_name, kwargs, current)
            if updated is None:
                continue
            if not isinstance(updated, dict):
                raise TypeError("on_send hook must return a headers dict")
            current = dict(updated)
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in current.items()):
            raise TypeError("headers must be dict[str, str]")
        return current

    async def send(self, actor, **kwargs) -> str:
        message_id = await self.send_with_options(actor, kwargs=kwargs)
        if message_id is None:
            raise RuntimeError("send did not enqueue a message")
        return message_id

    async def send_with_options(
        self,
        actor,
        *,
        kwargs: dict | None = None,
        delay: int | None = None,
        headers: dict | None = None,
        unique_key: str | None = None,
    ) -> str | None:
        if self._closed:
            raise RuntimeError("client is closed")
        kwargs = dict(kwargs or {})
        headers = self._apply_on_send(actor.name, kwargs, dict(headers or {}))
        payload = build_message(
            actor_name=actor.name,
            queue=actor.queue,
            kwargs=kwargs,
            headers=headers,
            max_retries=actor.max_retries,
            time_limit_ms=actor.time_limit,
            min_backoff_ms=actor.min_backoff,
            max_backoff_ms=actor.max_backoff,
            max_deliveries=actor.max_deliveries,
            lock_key=actor.lock_key,
            lock_ttl=actor.lock_ttl,
            skip_on_lock=actor.skip_on_lock,
            unique_key=unique_key,
        )
        if delay:
            payload["delay_ms"] = int(delay)
        if not self._push(payload):
            return None
        return payload["message_id"]

    def set_worker_alive(self, name: str, alive: bool = True) -> None:
        """Record whether ``worker_alive`` should report ``name``.

        A name that was never recorded is not alive. Passing ``False``
        removes it, so a test can show a dead worker.
        """

        if alive:
            self._alive.add(str(name))
        else:
            self._alive.discard(str(name))

    async def worker_alive(self, name: str) -> bool:
        return str(name) in self._alive

    async def close(self) -> None:
        self._closed = True

    async def acquire_lock(self, key: str, ttl: int) -> str | None:
        if key in self._locks:
            return None
        self._locks[key] = "memory"
        return "memory"

    async def release_lock(self, key: str, owner: str) -> bool:
        if self._locks.get(key) != owner:
            return False
        del self._locks[key]
        return True

    async def drain(self, app) -> None:
        await app.startup()
        try:
            while True:
                message = self._pop()
                if message is None:
                    return
                try:
                    result = await app.dispatch(message)
                except Exception:
                    self._release_unique(message)
                    raise
                outcome = result.get("outcome") if isinstance(result, dict) else None
                if outcome == "retry":
                    again = dict(message)
                    again["retries"] = int(again.get("retries") or 0) + 1
                    again.pop("delay_ms", None)
                    self._requeue(again)
                    continue
                self._release_unique(message)
        finally:
            await app.shutdown()

    def depth(self, queue: str) -> int:
        return len(self._queues.get(str(queue), ()))

    def _token(self, message: dict) -> tuple[str, str] | None:
        key = message.get("unique_key")
        if not isinstance(key, str) or not key:
            return None
        return (str(message.get("queue")), key)

    def _push(self, message: dict) -> bool:
        token = self._token(message)
        if token is not None and token in self._held:
            return False
        if token is not None:
            self._held.add(token)
        self._queues.setdefault(str(message["queue"]), []).append(message)
        return True

    def _pop(self) -> dict | None:
        for pending in self._queues.values():
            if pending:
                return pending.pop(0)
        return None

    def _requeue(self, message: dict) -> None:
        self._queues.setdefault(str(message["queue"]), []).append(message)

    def _release_unique(self, message: dict) -> None:
        token = self._token(message)
        if token is not None:
            self._held.discard(token)
