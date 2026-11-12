"""Actor declarations and the worker-side dispatch decision.

``@actor`` returns a declaration. It is not bound to an ``App`` or a connection.
``App`` holds the actor list, the worker-side hooks, and one client.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from simple_q.backoff import compute_backoff
from simple_q.message import CurrentMessage, bind_message, reset_message
from simple_q.metrics import finish_attempt, reject, start_attempt


def validate_queue(queue: str) -> str:
    if (
        not isinstance(queue, str)
        or not queue
        or any(
            not (("a" <= character <= "z") or ("0" <= character <= "9")) for character in queue
        )
    ):
        raise ValueError(f"queue {queue!r} must be a lowercase ascii word such as 'default'")
    return queue


def _delivery_count(message: dict) -> int:
    raw = message.get("deliveries")
    try:
        count = int(raw)
    except (TypeError, ValueError):
        return 1
    return count if count > 0 else 1


def _exception_text(caught: BaseException) -> str:
    return f"{type(caught).__name__}: {caught}"


def _dead_result(reason: str, error: str | None) -> dict:
    """Outcome dict for a dead delivery. ``error`` is what Rust stores."""

    return {"outcome": "dead", "reason": reason, "error": error}


log = logging.getLogger("simple_q")

DEFAULT_TIME_LIMIT_MS = 1_800_000
DEFAULT_MIN_BACKOFF_MS = 15_000
DEFAULT_MAX_BACKOFF_MS = 86_400_000 * 7
DEFAULT_MAX_DELIVERIES = 5
MESSAGE_VERSION = 1


@dataclass(frozen=True)
class DeadLetter:
    actor_name: str
    kwargs: dict
    reason: str
    message_id: str
    retries: int
    deliveries: int
    error: str | None


@dataclass(frozen=True)
class Actor:
    function: Any
    name: str
    queue: str
    max_retries: int
    time_limit: int
    min_backoff: int
    max_backoff: int
    lock_key: str | None
    lock_ttl: int
    skip_on_lock: bool
    max_deliveries: int


def actor(
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
    validate_queue(queue)
    if lock_ttl <= 0:
        raise ValueError("lock_ttl must be greater than zero")

    def decorate(function):
        return Actor(
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

    if fn is not None:
        return decorate(fn)
    return decorate


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
    unique_key: str | None = None,
) -> dict:
    validate_queue(queue)
    if lock_ttl <= 0:
        raise ValueError("lock_ttl must be greater than zero")
    if not isinstance(kwargs, dict):
        raise TypeError("kwargs must be a dict")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in headers.items()):
        raise TypeError("headers must be dict[str, str]")
    if unique_key is not None and not isinstance(unique_key, str):
        raise TypeError("unique_key must be a str")
    if unique_key == "":
        raise ValueError("unique_key must not be empty")
    message = {
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
    if unique_key is not None:
        message["unique_key"] = unique_key
    return message


class App:
    """Actor list, worker-side hooks, and one client.

    ``tracing=None`` leaves the OpenTelemetry layer uninstalled.
    A repeated actor name fails at construction.
    """

    def __init__(
        self,
        *,
        client,
        actors,
        on_start=None,
        on_stop=None,
        on_dead=None,
        around_dispatch=None,
        tracing=None,
    ):
        self.client = client
        self.tracing = tracing
        self.actors: dict[str, Actor] = {}
        for declared in actors:
            if declared.name in self.actors:
                raise ValueError(f"actor {declared.name!r} is already registered")
            self.actors[declared.name] = declared
        self._on_start = list(on_start or [])
        self._on_stop = list(on_stop or [])
        self._on_dead = list(on_dead or [])
        self._around_dispatch = list(around_dispatch or [])

    def on_start(self, fn):
        self._on_start.append(fn)
        return fn

    def on_stop(self, fn):
        self._on_stop.append(fn)
        return fn

    def on_dead(self, fn):
        self._on_dead.append(fn)
        return fn

    def around_dispatch(self, fn):
        """Wrap handler execution, including retry and dead decisions.

        ``fn`` is ``async fn(message, call_next)``. ``current_message()`` is
        already bound when the hook starts.
        """

        self._around_dispatch.append(fn)
        return fn

    async def startup(self) -> None:
        await self._run_hooks(self._on_start, "start")

    async def shutdown(self) -> None:
        await self._run_hooks(self._on_stop, "stop")

    async def dispatch(self, message: dict) -> dict:
        actor_name = str(message.get("actor_name") or "")
        declared = self.actors.get(actor_name)
        queue = message.get("queue") or (declared.queue if declared is not None else "default")
        kwargs = message.get("kwargs") if isinstance(message.get("kwargs"), dict) else {}
        current = CurrentMessage(
            message_id=str(message.get("message_id") or ""),
            retries=int(message.get("retries") or 0),
            max_retries=int(message.get("max_retries", declared.max_retries if declared else 0)),
            queue_name=str(queue),
            actor_name=actor_name,
            kwargs=dict(kwargs),
            headers={
                str(key): str(value) for key, value in (message.get("headers") or {}).items()
            },
            deliveries=_delivery_count(message),
            worker_name=str(message.get("worker_name") or ""),
        )
        token = bind_message(current)
        try:

            async def call_next():
                return await self._dispatch_bound(message, declared, current)

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
        deliveries: int = 0,
        *,
        error: str | None = None,
    ) -> None:
        """Record a dead-letter when Python dispatch did not run.

        ``error`` is ``"{type}: {message}"`` for retries and timeout, and
        ``None`` for poison, invalid, and unknown actors.
        """

        await self._dead(
            actor_name,
            dict(kwargs or {}),
            reason,
            str(message_id),
            int(retries),
            int(deliveries),
            error=error,
        )

    async def _dispatch_bound(self, message: dict, declared: Actor | None, current: CurrentMessage) -> dict:
        actor_name = current.actor_name
        if declared is None:
            await self._dead(
                actor_name,
                current.kwargs,
                "unknown_actor",
                current.message_id,
                current.retries,
                current.deliveries,
                error=None,
            )
            return _dead_result("unknown_actor", None)

        queue = current.queue_name
        message_id = current.message_id
        # Poison is `deliveries > max_deliveries`. The attempt at the limit still runs.
        deliveries = int(message.get("deliveries") or 1)
        if deliveries > int(message.get("max_deliveries") or declared.max_deliveries):
            await self._dead(
                actor_name,
                current.kwargs,
                "poison",
                message_id,
                current.retries,
                deliveries,
                error=None,
            )
            reject(queue, actor_name)
            return _dead_result("poison", None)

        start_attempt(message_id, queue, actor_name)
        owner = None
        lock_key = None
        error = None
        rejected = False
        try:
            lock_key = resolve_lock_key(message.get("lock_key") or declared.lock_key, current.kwargs)
            if lock_key:
                owner = await self.client.acquire_lock(
                    lock_key, _lock_ttl(message.get("lock_ttl"), declared.lock_ttl)
                )
                if owner is None:
                    if message.get("skip_on_lock", declared.skip_on_lock):
                        return {"outcome": "skipped"}
                    raise RuntimeError(f"lock busy: {lock_key}")
            try:
                timeout = int(message.get("time_limit_ms") or declared.time_limit) / 1000
                result = await asyncio.wait_for(declared.function(**current.kwargs), timeout)
            except (TimeoutError, asyncio.TimeoutError) as caught:
                error = caught
                if current.retries >= current.max_retries:
                    rejected = True
                    await self._dead(
                        actor_name,
                        current.kwargs,
                        "timeout",
                        message_id,
                        current.retries,
                        current.deliveries,
                        error=_exception_text(caught),
                    )
                    return _dead_result("timeout", _exception_text(caught))
                return await self._retry(message, declared, current)
            except Exception as caught:
                error = caught
                if current.retries >= current.max_retries:
                    rejected = True
                    await self._dead(
                        actor_name,
                        current.kwargs,
                        "retries",
                        message_id,
                        current.retries,
                        current.deliveries,
                        error=_exception_text(caught),
                    )
                    return _dead_result("retries", _exception_text(caught))
                return await self._retry(message, declared, current)
            else:
                return {"outcome": "ok", "result": result}
        finally:
            if owner is not None and lock_key is not None:
                await _release(self.client, lock_key, owner)
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

    async def _retry(self, message: dict, declared: Actor, current: CurrentMessage) -> dict:
        delay_ms = compute_backoff(
            current.retries,
            min_backoff=int(message.get("min_backoff_ms") or declared.min_backoff),
            max_backoff=int(message.get("max_backoff_ms") or declared.max_backoff),
        )
        return {"outcome": "retry", "delay_ms": delay_ms}

    async def _dead(
        self,
        actor_name: str,
        kwargs: dict,
        reason: str,
        message_id: str,
        retries: int,
        deliveries: int,
        *,
        error: str | None = None,
    ) -> None:
        letter = DeadLetter(
            actor_name=actor_name,
            kwargs=dict(kwargs),
            reason=reason,
            message_id=message_id,
            retries=retries,
            deliveries=deliveries,
            error=error,
        )
        for hook in self._on_dead:
            try:
                result = hook(letter)
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


def _lock_ttl(raw, declared: int) -> int:
    ttl = declared if raw is None else int(raw)
    if ttl <= 0:
        raise ValueError("lock_ttl must be greater than zero")
    return ttl


async def _release(client, lock_key: str, owner: str) -> None:
    """Unlock even when ``dispatch`` is already being cancelled.

    A cancel delivered at the ``await`` inside ``finally`` used to abandon
    the unlock. The inner task is shielded, the cancel count is cleared for
    that wait, and the cancellation is raised again after the unlock finishes.
    """

    task = asyncio.create_task(_release_once(client, lock_key, owner))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None:
            while current.cancelling():
                current.uncancel()
        try:
            await task
        finally:
            raise


async def _release_once(client, lock_key: str, owner: str) -> None:
    try:
        await client.release_lock(lock_key, owner)
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
    """Fill ``{field}`` placeholders in a lock key from the message kwargs.

    A resolved key is ASCII letters, digits, and ``:._-``, without a ``lock:`` prefix.
    ``demo:item:{name}`` is the template shape.
    """

    if not template:
        return None
    resolved = template
    if "{" in template:
        try:
            resolved = template.format_map(_FormatKwargs(kwargs))
        except KeyError as error:
            missing = error.args[0]
            raise KeyError(f"lock_key placeholder {missing} is missing from kwargs") from error
    if not usable_lock_key(resolved):
        raise ValueError(f"lock_key {resolved!r} is not a usable server key")
    return resolved


def usable_lock_key(key: str) -> bool:
    """A lock key is ASCII letters, digits, and ``:._-``, without a ``lock:`` prefix."""

    if not key or key.startswith("lock:"):
        return False
    return all(character.isascii() and (character.isalnum() or character in ":._-") for character in key)


def _bind_around(hook, message: dict, inner):
    async def wrapped():
        result = hook(message, inner)
        if inspect.isawaitable(result):
            return await result
        return result

    return wrapped
