"""Python facade for the queue runtime.

Importing this package does not open Redis and does not start a Tokio runtime.
Call ``configure(redis_url=...)`` before ``send``.
"""

from simple_q.message import current_message
from simple_q.registry import Registry, build_message, configure

__all__ = ["Registry", "build_message", "configure", "current_message"]
