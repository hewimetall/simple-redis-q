"""Python facade for the queue runtime.

Importing this package does not open Redis and does not start a Tokio runtime.
Call ``configure(redis_url=...)`` before ``send``.
"""

from simple_q.memory import disable_memory, drain, enable_memory
from simple_q.message import current_message
from simple_q.registry import Registry, build_message, configure
from simple_q.tracing import Tracing

__all__ = [
    "Registry",
    "Tracing",
    "build_message",
    "configure",
    "current_message",
    "disable_memory",
    "drain",
    "enable_memory",
]
