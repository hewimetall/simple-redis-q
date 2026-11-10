"""Python facade for the queue runtime.

Importing this package does not open Redis. ``Client`` opens a connection on
the first send. ``@actor`` only records a declaration.
"""

from simple_q.dispatch import (
    MESSAGE_VERSION,
    Actor,
    App,
    DeadLetter,
    actor,
    build_message,
)
from simple_q.memory import MemoryClient
from simple_q.message import current_message, worker_name
from simple_q.tracing import Tracing


def __getattr__(name: str):
    if name in {"Client", "Config"}:
        from simple_q import _runtime

        value = getattr(_runtime, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Actor",
    "App",
    "Client",
    "Config",
    "DeadLetter",
    "MemoryClient",
    "Tracing",
    "actor",
    "build_message",
    "current_message",
    "worker_name",
    "MESSAGE_VERSION",
]
