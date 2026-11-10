import asyncio


async def tracked(awaitable, name: str):
    """Name the running asyncio task so the supervisor can request cancellation.

    The name is diagnostic. Cancelling it does not stop native code that holds
    the interpreter, and it is not a business failure mark.
    """

    current = asyncio.current_task()
    if current is not None:
        current.set_name(name)
    return await awaitable
