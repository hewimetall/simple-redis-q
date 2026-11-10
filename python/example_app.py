"""Factory used by the local compose worker.

The factory reads the Redis URL. The library does not.
"""

from __future__ import annotations

import os

from simple_q import App, Client, Config, actor


@actor(queue="default", max_retries=3)
async def recalc(project_id: str) -> None:
    return None


def create_app() -> App:
    config = Config.from_env()
    return App(
        client=Client(os.environ["REDIS_URL"], config=config),
        actors=[recalc],
    )
