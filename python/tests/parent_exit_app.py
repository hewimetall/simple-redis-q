"""One task that waits for a file. The pidfd test kills the supervisor mid-task."""

from __future__ import annotations

import asyncio
import os

from simple_q import Actor, App, Client, Config


async def gate():
    started = os.environ["GATE_STARTED"]
    with open(started, "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))
    release = os.environ["GATE_RELEASE"]
    while not os.path.exists(release):
        await asyncio.sleep(0.05)
    return {"ok": True}


def create_app() -> App:
    queue = os.environ["PARENT_EXIT_QUEUE"]
    actor = Actor(
        function=gate,
        name="gate",
        queue=queue,
        max_retries=0,
        time_limit=120_000,
        min_backoff=15_000,
        max_backoff=86_400_000 * 7,
        lock_key=None,
        lock_ttl=30,
        skip_on_lock=False,
        max_deliveries=2,
    )
    return App(
        client=Client(os.environ["REDIS_URL"], config=Config.from_env()),
        actors=[actor],
    )
