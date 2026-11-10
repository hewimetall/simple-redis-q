"""A module whose only queue API is ``@actor``.

Importing it must not construct an ``App`` and must not open Redis.
"""

from simple_q import actor


@actor(queue="default", max_retries=3)
async def recalc(project_id: str) -> None:
    return None
