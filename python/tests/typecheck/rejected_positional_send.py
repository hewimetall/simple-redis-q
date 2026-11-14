from simple_q import MemoryClient, actor


@actor
async def work(value: str) -> int:
    return len(value)


async def bad(client: MemoryClient) -> None:
    await client.send(work, "nope")
