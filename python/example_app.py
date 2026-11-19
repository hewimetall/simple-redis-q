"""Factory used by the local compose worker.

The factory reads SIMPLE_Q_APP_REDIS_URL. The supervisor removes REDIS_URL before the child starts. A process that is not a supervised child can still read REDIS_URL.
"""

from __future__ import annotations

import os

from simple_q import App, Client, Config, actor


@actor(queue="default", max_retries=3)
async def render_report(report_id: str) -> None:
    return None


def create_app() -> App:
    config = Config.from_env()
    return App(
        client=Client(
            os.environ.get("SIMPLE_Q_APP_REDIS_URL") or os.environ["REDIS_URL"],
            config=config,
        ),
        actors=[render_report],
    )
