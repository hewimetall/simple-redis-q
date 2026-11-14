from simple_q import actor


@actor
async def work(value: str) -> int:
    return len(value)


async def bad() -> None:
    await work.function(1)
