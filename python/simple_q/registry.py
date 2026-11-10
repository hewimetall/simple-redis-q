from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from typing import Any

from simple_q.backoff import compute_backoff
from simple_q.message import CurrentMessage, bind_message, reset_message
from simple_q.metrics import finish_attempt, reject, start_attempt

log = logging.getLogger("simple_q")

DEFAULT_TIME_LIMIT_MS = 1_800_000
DEFAULT_MIN_BACKOFF_MS = 15_000
DEFAULT_MAX_BACKOFF_MS = 86_400_000 * 7
DEFAULT_MAX_DELIVERIES = 5
MESSAGE_VERSION = 1


class Actor:
    def __init__(
        self,
        function,
        *,
        name: str,
        queue: str,
        max_retries: int,
        time_limit: int,
        min_backoff: int,
        max_backoff: int,
        lock_key: str | None,
        lock_ttl: int,
        skip_on_lock: bool,
        max_deliveries: int,
    ):
        self.function = function
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.time_limit = time_limit
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.lock_key = lock_key
        self.lock_ttl = lock_ttl
        self.skip_on_lock = skip_on_lock
        self.max_deliveries = max_deliveries

    async def send(self, **kwargs) -> str:
        return await self.send_with_options(kwargs=kwargs)

    async def send_with_options(
        self,
        *,
        kwargs: dict | None = None,
        delay: int | None = None,
        headers: dict | None = None,
    ) -> str:
        from simple_q_runtime import enqueue, is_configured

        if not is_configured():
            raise RuntimeError("call simple_q.configure(redis_url=...) before send")
        payload = build_message(
            actor_name=self.name,
            queue=self.queue,
            kwargs=kwargs or {},
            headers=headers or {},
            max_retries=self.max_retries,
            time_limit_ms=self.time_limit,
            min_backoff_ms=self.min_backoff,
            max_backoff_ms=self.max_backoff,
            max_deliveries=self.max_deliveries,
            lock_key=self.lock_key,
            lock_ttl=self.lock_ttl,
            skip_on_lock=self.skip_on_lock,
        )
        import json

        return await enqueue(json.dumps(payload), delay)


def build_message(
    *,
    actor_name: str,
    queue: str,
    kwargs: dict,
    headers: dict,
    max_retries: int,
    time_limit_ms: int = DEFAULT_TIME_LIMIT_MS,
    min_backoff_ms: int = DEFAULT_MIN_BACKOFF_MS,
    max_backoff_ms: int = DEFAULT_MAX_BACKOFF_MS,
    max_deliveries: int = DEFAULT_MAX_DELIVERIES,
    retries: int = 0,
    message_id: str | None = None,
    lock_key: str | None = None,
    lock_ttl: int = 30,
    skip_on_lock: bool = False,
) -> dict:
    if not isinstance(kwargs, dict):
        raise TypeError("kwargs must be a dict")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in headers.items()):
        raise TypeError("headers must be dict[str, str]")
    return {
        "version": MESSAGE_VERSION,
        "message_id": message_id or str(uuid.uuid4()),
        "actor_name": actor_name,
        "queue": queue,
        "kwargs": kwargs,
        "headers": dict(headers),
        "retries": retries,
        "max_retries": max_retries,
        "time_limit_ms": time_limit_ms,
        "min_backoff_ms": min_backoff_ms,
        "max_backoff_ms": max_backoff_ms,
        "max_deliveries": max_deliveries,
        "lock_key": lock_key,
        "lock_ttl": lock_ttl,
        "skip_on_lock": skip_on_lock,
    }


class Registry:
    """Actor registry loaded by ``--app module:attribute``."""

    def __init__(self):
        self.actors: dict[str, Actor] = {}
        self._on_start = []
        self._on_stop = []
        self._on_dead = []

    def actor(
        self,
        fn=None,
        *,
        name: str | None = None,
        queue: str = "default",
        max_retries: int = 0,
        time_limit: int = DEFAULT_TIME_LIMIT_MS,
        min_backoff: int = DEFAULT_MIN_BACKOFF_MS,
        max_backoff: int = DEFAULT_MAX_BACKOFF_MS,
        lock_key: str | None = None,
        lock_ttl: int = 30,
        skip_on_lock: bool = False,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
    ):
        def decorate(function):
            actor = Actor(
                function,
                name=name or function.__name__,
                queue=queue,
                max_retries=max_retries,
                time_limit=time_limit,
                min_backoff=min_backoff,
                max_backoff=max_backoff,
                lock_key=lock_key,
                lock_ttl=lock_ttl,
                skip_on_lock=skip_on_lock,
                max_deliveries=max_deliveries,
            )
            self.actors[actor.name] = actor
            return actor

        if fn is not None:
            return decorate(fn)
        return decorate

    def on_start(self, fn):
        self._on_start.append(fn)
        return fn

    def on_stop(self, fn):
        self._on_stop.append(fn)
        return fn

    def on_dead(self, fn):
        self._on_dead.append(fn)
        return fn

    async def startup(self) -> None:
        await self._run_hooks(self._on_start, "start")

    async def shutdown(self) -> None:
        await self._run_hooks(self._on_stop, "stop")

    async def execute(self, task_name: str, payload: dict):
        actor = self.actors[task_name]
        return await actor.function(**(payload or {}))

    async def dispatch(self, message: dict) -> dict:
        actor_name = message["actor_name"]
        actor = self.actors.get(actor_name)
        if actor is None:
            raise KeyError(actor_name)
        queue = message.get("queue") or actor.queue
        message_id = message["message_id"]
        deliveries = int(message.get("deliveries") or 1)
        if deliveries > int(message.get("max_deliveries") or actor.max_deliveries):
            await self._dead(actor_name, message.get("kwargs") or {}, "poison")
            reject(queue, actor_name)
            return {"outcome": "dead", "reason": "poison"}

        start_attempt(message_id, queue, actor_name)
        current = CurrentMessage(
            message_id=message_id,
            retries=int(message.get("retries") or 0),
            max_retries=int(message.get("max_retries", actor.max_retries)),
            queue_name=queue,
            actor_name=actor_name,
            kwargs=dict(message.get("kwargs") or {}),
            headers={str(key): str(value) for key, value in (message.get("headers") or {}).items()},
        )
        token = bind_message(current)
        owner = None
        lock_key = message.get("lock_key") or actor.lock_key
        error = None
        rejected = False
        try:
            if lock_key:
                from simple_q_runtime import acquire_lock

                owner = await acquire_lock(lock_key, int(message.get("lock_ttl") or actor.lock_ttl))
                if owner is None:
                    if message.get("skip_on_lock", actor.skip_on_lock):
                        return {"outcome": "skipped"}
                    raise RuntimeError(f"lock busy: {lock_key}")
            try:
                timeout = int(message.get("time_limit_ms") or actor.time_limit) / 1000
                result = await asyncio.wait_for(actor.function(**current.kwargs), timeout)
            except (TimeoutError, asyncio.TimeoutError) as caught:
                error = caught
                if current.retries >= current.max_retries:
                    rejected = True
                    await self._dead(actor_name, current.kwargs, "timeout")
                    return {"outcome": "dead", "reason": "timeout"}
                return await self._retry(message, actor, current)
            except Exception as caught:
                error = caught
                if current.retries >= current.max_retries:
                    rejected = True
                    return {"outcome": "dead", "reason": "retries"}
                return await self._retry(message, actor, current)
            else:
                return {"outcome": "ok", "result": result}
        finally:
            if owner is not None and lock_key is not None:
                await _release(lock_key, owner)
            finish_attempt(
                message_id,
                queue,
                actor_name,
                error=error is not None,
                rejected=rejected,
            )
            reset_message(token)

    async def _retry(self, message: dict, actor: Actor, current: CurrentMessage) -> dict:
        delay_ms = compute_backoff(
            current.retries,
            min_backoff=int(message.get("min_backoff_ms") or actor.min_backoff),
            max_backoff=int(message.get("max_backoff_ms") or actor.max_backoff),
        )
        return {"outcome": "retry", "delay_ms": delay_ms}

    async def _dead(self, actor_name: str, kwargs: dict, reason: str) -> None:
        for hook in self._on_dead:
            try:
                result = hook(actor_name, kwargs, reason)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("dead hook failed for %s", actor_name)

    async def _run_hooks(self, hooks, label: str) -> None:
        for hook in hooks:
            try:
                result = hook()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("%s hook failed", label)


async def _release(lock_key: str, owner: str) -> None:
    try:
        from simple_q_runtime import release_lock

        await release_lock(lock_key, owner)
    except Exception:
        log.warning("lock release failed for %s; the task result stands", lock_key, exc_info=True)


def configure(redis_url: str) -> None:
    from simple_q_runtime import install

    install(redis_url)
