import builtins
import typing

__embedded__: builtins.bool

class Config:
    heartbeat_secs: builtins.int
    missed_heartbeats: builtins.int
    ping_budget_secs: builtins.int
    done_ttl_secs: builtins.int
    dead_ttl_secs: builtins.int
    unique_ttl_secs: builtins.int
    max_deliveries: builtins.int
    memory_soft_ratio: builtins.float
    memory_hard_ratio: builtins.float
    child_memory_soft: builtins.int | None
    child_memory_hard: builtins.int | None
    def __init__(self) -> None: ...
    @classmethod
    def from_env(cls) -> Config: ...

class Client:
    def __init__(self, url: builtins.str, config: Config) -> None: ...
    def on_send(self, hook: typing.Callable) -> typing.Callable: ...
    def send(self, actor: typing.Any, **kwargs: typing.Any) -> typing.Awaitable[builtins.str]: ...
    def send_with_options(
        self,
        actor: typing.Any,
        *,
        kwargs: builtins.dict | None = ...,
        delay: builtins.int | None = ...,
        headers: builtins.dict | None = ...,
        unique_key: builtins.str | None = ...,
    ) -> typing.Awaitable[builtins.str | None]: ...
    def worker_alive(self, name: builtins.str) -> typing.Awaitable[builtins.bool]: ...
    def close(self) -> typing.Awaitable[None]: ...
    def acquire_lock(
        self, key: builtins.str, ttl_seconds: builtins.int
    ) -> typing.Awaitable[builtins.str | None]: ...
    def release_lock(
        self, key: builtins.str, owner: builtins.str
    ) -> typing.Awaitable[builtins.bool]: ...

class Admin:
    def __init__(self, client: Client) -> None: ...
    def snapshot(
        self, queues: builtins.list[builtins.str], host: builtins.str | None = ...
    ) -> typing.Awaitable[builtins.str]: ...
    def dead(
        self,
        queue: builtins.str,
        limit: builtins.int = 50,
        cursor: builtins.str | None = None,
    ) -> typing.Awaitable[builtins.str]: ...
    def peek(
        self,
        queue: builtins.str,
        state: builtins.str,
        limit: builtins.int = 50,
        cursor: builtins.str | None = None,
    ) -> typing.Awaitable[builtins.str]: ...

def execution_snapshot() -> builtins.list[builtins.str]: ...
