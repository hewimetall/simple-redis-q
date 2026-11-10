"""Application used by the orphan-window integration tests."""

from __future__ import annotations

import asyncio
import json
import os
import time

from simple_q import Registry

app = Registry()


def _append(path: str, payload: dict) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


@app.actor(name="hold", queue="holdq", max_retries=0, time_limit=120_000, max_deliveries=5)
async def hold():
    path = os.environ["HOLD_TOUCH"]
    _append(path, {"pid": os.getpid()})
    await asyncio.sleep(int(os.environ.get("HOLD_SECONDS", "25")))
    return {"ok": True}


def _hold_gil(seconds: int) -> None:
    """Busy Python loop. It holds the GIL except for the clock reads."""

    deadline = time.perf_counter() + seconds
    total = 0
    while time.perf_counter() < deadline:
        for value in range(20_000):
            total += value
    if total < 0:
        raise RuntimeError("unreachable")


@app.actor(name="gil", queue="gilq", max_retries=0, time_limit=120_000, max_deliveries=5)
async def gil():
    path = os.environ["GIL_TOUCH"]
    _append(path, {"pid": os.getpid()})
    _hold_gil(int(os.environ.get("GIL_SECONDS", "20")))
    return {"ok": True}


@app.actor(name="die", queue="killq", max_retries=0, time_limit=120_000, max_deliveries=1)
async def die():
    path = os.environ["KILL_TOUCH"]
    _append(path, {"pid": os.getpid()})
    await asyncio.sleep(3600)


@app.on_dead
def remember(actor_name, kwargs, reason, message_id, retries):
    path = os.environ.get("DEAD_LOG")
    if not path:
        return
    _append(
        path,
        {
            "actor_name": actor_name,
            "reason": reason,
            "message_id": message_id,
            "retries": retries,
        },
    )
