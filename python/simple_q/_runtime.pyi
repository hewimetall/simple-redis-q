from collections.abc import Awaitable
from typing import Any, ParamSpec, TypeVar, overload

from simple_q.dispatch import Actor
from simple_q.interfaces import OnSend

P = ParamSpec("P")
R = TypeVar("R")
S = TypeVar("S", bound=OnSend)

__embedded__: bool


class Config:
    heartbeat_secs: int
    missed_heartbeats: int
    poll_interval_ms: int
    ping_interval_ms: int
    ping_timeout_ms: int
    lifecycle_interval_ms: int
    sweep_interval_secs: int
    sweep_batch: int
    batch_size: int
    idempotency_wait_ms: int
    idempotency_wait_max_ms: int
    ping_budget_secs: int
    done_ttl_secs: int
    dead_ttl_secs: int
    unique_ttl_secs: int
    max_deliveries: int
    memory_soft_ratio: float
    memory_hard_ratio: float
    child_memory_soft: int | None
    child_memory_hard: int | None

    def __init__(self) -> None: ...
    @classmethod
    def from_env(cls) -> Config: ...


class Client:
    def __init__(self, url: str, config: Config) -> None: ...
    @property
    def config(self) -> Config: ...
    def on_send(self, fn: S) -> S: ...
    def send(self, actor: Actor[P, R], **kwargs: Any) -> Awaitable[str]: ...
    def send_with_options(
        self,
        actor: Actor[P, R],
        *,
        kwargs: dict[str, Any] | None = ...,
        delay: int | None = ...,
        headers: dict[str, str] | None = ...,
        unique_key: str | None = ...,
    ) -> Awaitable[str | None]: ...
    def worker_alive(self, name: str) -> Awaitable[bool]: ...
    def close(self) -> Awaitable[None]: ...
    @overload
    def acquire_lock(self, key: str, ttl_seconds: int) -> Awaitable[str | None]: ...
    @overload
    def acquire_lock(self, key: str, *, ttl: int) -> Awaitable[str | None]: ...
    def release_lock(self, key: str, owner: str) -> Awaitable[bool]: ...


class Admin:
    def __init__(self, client: Client) -> None: ...
    def snapshot(
        self, queues: list[str], host: str | None = ...
    ) -> Awaitable[dict[str, Any]]: ...
    def dead(
        self,
        queue: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Awaitable[dict[str, Any]]: ...
    def peek(
        self,
        queue: str,
        state: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Awaitable[dict[str, Any]]: ...


def execution_snapshot() -> list[str]:
    """One formatted line per in-flight execution of this process."""
    ...
