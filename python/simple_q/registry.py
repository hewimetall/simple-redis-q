from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from typing import Any

from simple_q.backoff import compute_backoff
from simple_q.message import CurrentMessage, bind_message, reset_message
from simple_q.metrics import finish_attempt, reject, start_attempt
from simple_q.tracing import Tracing

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
        registry: Registry | None = None,
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
        self.registry = registry

    async def send(self, **kwargs) -> str:
        return await self.send_with_options(kwargs=kwargs)

    async def send_with_options(
        self,
        *,
        kwargs: dict | None = None,
        delay: int | None = None,
        headers: dict | None = None,
    ) -> str:
        kwargs = dict(kwargs or {})
        headers = dict(headers or {})
        if self.registry is not None:
            headers = self.registry.apply_on_send(self.name, kwargs, headers)
        payload = build_message(
            actor_name=self.name,
            queue=self.queue,
            kwargs=kwargs,
            headers=headers,
            max_retries=self.max_retries,
            time_limit_ms=self.time_limit,
            min_backoff_ms=self.min_backoff,
            max_backoff_ms=self.max_backoff,
            max_deliveries=self.max_deliveries,
            lock_key=self.lock_key,
            lock_ttl=self.lock_ttl,
            skip_on_lock=self.skip_on_lock,
        )
        from simple_q.memory import enabled, push

        if enabled():
            push(payload)
            return payload["message_id"]
        from simple_q_runtime import enqueue, is_configured

        if not is_configured():
            raise RuntimeError("call simple_q.configure(redis_url=...) before send")
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
    """Actor registry loaded by ``--app module:attribute``.

    ``tracing=None`` is the default. The child then does not install an
    OpenTelemetry layer and does not call into Python to export spans.
    """

    def __init__(self, tracing: Tracing | None = None):
        self.tracing = tracing
        self.actors: dict[str, Actor] = {}
        self._on_start = []
        self._on_stop = []
        self._on_dead = []
        self._on_send = []
        self._around_dispatch = []

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
                registry=self,
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

    def on_send(self, fn):
        """Adjust headers before enqueue, on the caller's task.

        The hook is synchronous and runs before the Rust boundary, so a
        ``contextvars.ContextVar`` set by the caller is visible here.
        Return the headers mapping to store on the message.
        """

        self._on_send.append(fn)
        return fn

    def around_dispatch(self, fn):
        """Wrap handler execution, including retry and dead decisions.

        ``fn`` is ``async fn(message, call_next)``. ``current_message()`` is
        already bound when the hook starts.
        """

        self._around_dispatch.append(fn)
        return fn

    def apply_on_send(self, actor_name: str, kwargs: dict, headers: dict) -> dict:
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

    async def startup(self) -> None:
        await self._run_hooks(self._on_start, "start")

    async def shutdown(self) -> None:
        await self._run_hooks(self._on_stop, "stop")

    async def execute(self, task_name: str, payload: dict):
        actor = self.actors[task_name]
        return await actor.function(**(payload or {}))

    async def dispatch(self, message: dict) -> dict:
        actor_name = str(message.get("actor_name") or "")
        actor = self.actors.get(actor_name)
        queue = message.get("queue") or (actor.queue if actor is not None else "default")
        kwargs = message.get("kwargs") if isinstance(message.get("kwargs"), dict) else {}
        current = CurrentMessage(
            message_id=str(message.get("message_id") or ""),
            retries=int(message.get("retries") or 0),
            max_retries=int(message.get("max_retries", actor.max_retries if actor else 0)),
            queue_name=str(queue),
            actor_name=actor_name,
            kwargs=dict(kwargs),
            headers={
                str(key): str(value) for key, value in (message.get("headers") or {}).items()
            },
        )
        token = bind_message(current)
        try:

            async def call_next():
                return await self._dispatch_bound(message, actor, current)

            return await self._run_around(message, call_next)
        finally:
            reset_message(token)

    async def note_dead(
        self,
        actor_name: str,
        kwargs: dict,
        reason: str,
        message_id: str,
        retries: int,
    ) -> None:
        """Record a dead-letter from the worker when Python dispatch did not run."""

        await self._dead(actor_name, dict(kwargs or {}), reason, str(message_id), int(retries))

    async def _dispatch_bound(self, message: dict, actor: Actor | None, current: CurrentMessage) -> dict:
        actor_name = current.actor_name
        if actor is None:
            await self._dead(
                actor_name,
                current.kwargs,
                "unknown_actor",
                current.message_id,
                current.retries,
            )
            return {"outcome": "dead", "reason": "unknown_actor"}

        queue = current.queue_name
        message_id = current.message_id
        deliveries = int(message.get("deliveries") or 1)
        if deliveries > int(message.get("max_deliveries") or actor.max_deliveries):
            await self._dead(actor_name, current.kwargs, "poison", message_id, current.retries)
            reject(queue, actor_name)
            return {"outcome": "dead", "reason": "poison"}

        start_attempt(message_id, queue, actor_name)
        owner = None
        lock_key = None
        error = None
        rejected = False
        try:
            lock_key = resolve_lock_key(message.get("lock_key") or actor.lock_key, current.kwargs)
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
                    await self._dead(actor_name, current.kwargs, "timeout", message_id, current.retries)
                    return {"outcome": "dead", "reason": "timeout"}
                return await self._retry(message, actor, current)
            except Exception as caught:
                error = caught
                if current.retries >= current.max_retries:
                    rejected = True
                    await self._dead(actor_name, current.kwargs, "retries", message_id, current.retries)
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

    async def _run_around(self, message: dict, call_next):
        chain = call_next
        for hook in reversed(self._around_dispatch):
            chain = _bind_around(hook, message, chain)
        return await chain()

    async def _retry(self, message: dict, actor: Actor, current: CurrentMessage) -> dict:
        delay_ms = compute_backoff(
            current.retries,
            min_backoff=int(message.get("min_backoff_ms") or actor.min_backoff),
            max_backoff=int(message.get("max_backoff_ms") or actor.max_backoff),
        )
        return {"outcome": "retry", "delay_ms": delay_ms}

    async def _dead(
        self,
        actor_name: str,
        kwargs: dict,
        reason: str,
        message_id: str,
        retries: int,
    ) -> None:
        for hook in self._on_dead:
            try:
                result = hook(actor_name, kwargs, reason, message_id, retries)
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


class _FormatKwargs(dict):
    def __missing__(self, key):
        raise KeyError(key)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        if isinstance(value, str):
            return value
        return str(value)


def resolve_lock_key(template: str | None, kwargs: dict) -> str | None:
    """Fill ``{field}`` placeholders in a lock key from the message kwargs."""

    if not template:
        return None
    resolved = template
    if "{" in template:
        try:
            resolved = template.format_map(_FormatKwargs(kwargs))
        except KeyError as error:
            missing = error.args[0]
            raise KeyError(f"lock_key placeholder {missing} is missing from kwargs") from error
    if (
        not resolved
        or resolved.startswith("lock:")
        or any(character.isspace() for character in resolved)
    ):
        raise ValueError(f"lock_key {resolved!r} is not a usable server key")
    return resolved


def _bind_around(hook, message: dict, inner):
    async def wrapped():
        result = hook(message, inner)
        if inspect.isawaitable(result):
            return await result
        return result

    return wrapped


def configure(redis_url: str) -> None:
    from simple_q_runtime import install

    install(redis_url)
