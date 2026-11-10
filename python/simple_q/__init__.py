"""Python facade for the queue runtime.

Importing this package does not open Redis and does not start a Tokio runtime.
Call ``configure(redis_url=...)`` before ``send``.
"""

from simple_q.memory import disable_memory, drain, enable_memory
from simple_q.message import current_message, worker_name
from simple_q.registry import Registry, build_message, configure
from simple_q.tracing import Tracing


def _queue_of_worker_name(name: object) -> str | None:
    if not isinstance(name, str) or not name.startswith("agent-worker."):
        return None
    fields = name.split(".")
    if len(fields) < 5 or fields[0] != "agent-worker":
        return None
    queue = fields[1]
    if not queue or any(
        not (("a" <= character <= "z") or ("0" <= character <= "9")) for character in queue
    ):
        return None
    return queue


def worker_alive(name: str) -> bool:
    """Return whether ``name`` still has a fresh heartbeat.

    The process calling worker_alive must use the same SIMPLE_Q_HEARTBEAT_SECS
    and SIMPLE_Q_MISSED_HEARTBEATS as the workers.
    """

    from simple_q.memory import enabled

    if enabled():
        return True
    if _queue_of_worker_name(name) is None:
        return False
    from simple_q_runtime import is_configured, worker_alive as probe

    if not is_configured():
        return False
    return bool(probe(name))


__all__ = [
    "Registry",
    "Tracing",
    "build_message",
    "configure",
    "current_message",
    "disable_memory",
    "drain",
    "enable_memory",
    "worker_alive",
    "worker_name",
]
