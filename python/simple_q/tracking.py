import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


async def tracked(awaitable: Awaitable[T], name: str) -> T:
    """Name the running asyncio task so the supervisor can request cancellation.

    The name is diagnostic. Cancelling it does not stop native code that holds
    the interpreter, and it is not a business failure mark.
    """

    current = asyncio.current_task()
    if current is not None:
        current.set_name(name)
    return await awaitable
