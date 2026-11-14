"""Structural contracts for the queue client, the worker app, and hooks.

``simple_q.Client`` is the Redis implementation. This module's ``Client`` is
the protocol both that class and ``MemoryClient`` satisfy. Neither class
inherits from it.

``send`` stays keyword-only. Pyright rejects ``**kwargs: P.kwargs`` unless
``*args: P.args`` is present too, and that form also accepts positional calls
this method does not run. Keyword values are therefore ``Any``. The handler
signature stays on ``Actor.function``.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, ParamSpec, Protocol, TypeVar, overload

if TYPE_CHECKING:
    from simple_q.dispatch import Actor, DeadLetter
    from simple_q.tracing import Tracing

P = ParamSpec("P")
R = TypeVar("R")
S = TypeVar("S", bound="OnSend")
L = TypeVar("L", bound="LifecycleHook")
D = TypeVar("D", bound="DeadHook")
A = TypeVar("A", bound="AroundDispatch")


class OnSend(Protocol):
    """Synchronous hook run before a message is stored.

    Return the headers to keep, or ``None`` to keep the previous mapping.
    """

    def __call__(
        self,
        actor_name: str,
        kwargs: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, str] | None: ...


class LifecycleHook(Protocol):
    """``on_start`` and ``on_stop``. Sync or async, no arguments."""

    def __call__(self) -> Awaitable[None] | None: ...


class DeadHook(Protocol):
    """``on_dead``. Sync or async. The only argument is the letter."""

    def __call__(self, letter: DeadLetter) -> Awaitable[None] | None: ...


class CallNext(Protocol):
    """The rest of dispatch, including the handler and the retry decision."""

    def __call__(self) -> Awaitable[dict[str, Any]]: ...


class AroundDispatch(Protocol):
    """``around_dispatch``. Sync or async.

    ``message`` is the dict the worker passed to ``dispatch``.
    """

    def __call__(
        self,
        message: dict[str, Any],
        call_next: CallNext,
    ) -> Awaitable[dict[str, Any]] | dict[str, Any]: ...


class Client(Protocol):
    """What ``App`` and a producer can call. No new operations."""

    def on_send(self, fn: S) -> S: ...

    def send(self, actor: Actor[P, R], **kwargs: Any) -> Awaitable[str]: ...

    def send_with_options(
        self,
        actor: Actor[P, R],
        *,
        kwargs: dict[str, Any] | None = None,
        delay: int | None = None,
        headers: dict[str, str] | None = None,
        unique_key: str | None = None,
    ) -> Awaitable[str | None]: ...

    def worker_alive(self, name: str) -> Awaitable[bool]: ...

    def close(self) -> Awaitable[None]: ...

    @overload
    def acquire_lock(self, key: str, ttl_seconds: int) -> Awaitable[str | None]: ...

    @overload
    def acquire_lock(self, key: str, *, ttl: int) -> Awaitable[str | None]: ...

    def release_lock(self, key: str, owner: str) -> Awaitable[bool]: ...


class WorkerApp(Protocol):
    """What the worker and a middleware ``install`` can call.

    The child calls ``startup``, ``shutdown``, ``dispatch``, and ``note_dead``.
    Dispatch still takes and returns a dict.
    """

    client: Client
    tracing: Tracing | None

    async def startup(self) -> None: ...

    async def shutdown(self) -> None: ...

    async def dispatch(self, message: dict[str, Any]) -> dict[str, Any]: ...

    async def note_dead(
        self,
        actor_name: str,
        kwargs: dict[str, Any],
        reason: str,
        message_id: str,
        retries: int,
        deliveries: int = 0,
        *,
        error: str | None = None,
    ) -> None: ...

    def on_start(self, fn: L) -> L: ...

    def on_stop(self, fn: L) -> L: ...

    def on_dead(self, fn: D) -> D: ...

    def around_dispatch(self, fn: A) -> A: ...


class Middleware(Protocol):
    """Optional object with ``install``. There is no required base class."""

    def install(self, client: Client, app: WorkerApp) -> None: ...
