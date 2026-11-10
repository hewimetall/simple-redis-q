"""Read-only snapshot of supervisors, processes, and the tasks they hold.

``import simple_q`` does not load this module. Import it directly:

``from simple_q.admin import QueueAdmin``
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from simple_q._runtime import Admin


@dataclass(frozen=True)
class HeldTask:
    queue: str
    task_id: str
    message_id: str
    actor_name: str
    kwargs: dict
    retries: int
    max_retries: int
    time_limit_ms: int
    locked_at: int | None
    deliveries: int | None


@dataclass(frozen=True)
class ProcessInfo:
    host: str
    pid: int
    ppid: int | None
    slot: int | None
    state: str
    reason: str | None
    exit: int | None
    rss: int | None
    started_at: int
    ended_at: int | None
    tasks_done: int | None
    max_tasks: int | None
    workers: list[str]
    last_seen: int | None
    alive: bool
    tasks: list[HeldTask]


@dataclass(frozen=True)
class QueueStats:
    name: str
    active: int
    scheduled: int
    inflight: int
    done: int
    dead: int
    workers: int
    workers_alive: int


@dataclass(frozen=True)
class SupervisorInfo:
    host: str
    pid: int
    started_at: int
    last_seen: int
    version: str
    queues: list[str]
    processes: int
    concurrency: int
    max_tasks: int
    child_soft: int | None
    child_hard: int | None
    cgroup_current: int | None
    cgroup_max: int | None
    recycled: int
    drains: int
    kills: int
    restarts: int
    children: list[ProcessInfo]


@dataclass(frozen=True)
class Snapshot:
    taken_at: int
    queues: list[QueueStats]
    supervisors: list[SupervisorInfo]
    orphans: list[ProcessInfo]


class QueueAdmin:
    """Read a snapshot through the caller's ``Client`` connection."""

    def __init__(self, client) -> None:
        self._admin = Admin(client)

    async def snapshot(self, queues: list[str], host: str | None = None) -> Snapshot:
        raw = await self._admin.snapshot(list(queues), host)
        payload = json.loads(raw)
        return _snapshot(payload)


def _opt_int(value) -> int | None:
    if value is None:
        return None
    return int(value)


def _held(value: dict) -> HeldTask:
    deliveries = value.get("deliveries")
    return HeldTask(
        queue=value["queue"],
        task_id=value["task_id"],
        message_id=value.get("message_id") or "",
        actor_name=value.get("actor_name") or "",
        kwargs=dict(value.get("kwargs") or {}),
        retries=int(value.get("retries") or 0),
        max_retries=int(value.get("max_retries") or 0),
        time_limit_ms=int(value.get("time_limit_ms") or 0),
        locked_at=_opt_int(value.get("locked_at")),
        deliveries=_opt_int(deliveries),
    )


def _process(value: dict) -> ProcessInfo:
    return ProcessInfo(
        host=value.get("host") or "",
        pid=int(value["pid"]),
        ppid=_opt_int(value.get("ppid")),
        slot=_opt_int(value.get("slot")),
        state=value.get("state") or "",
        reason=value.get("reason"),
        exit=_opt_int(value.get("exit")),
        rss=_opt_int(value.get("rss")),
        started_at=int(value.get("started_at") or 0),
        ended_at=_opt_int(value.get("ended_at")),
        tasks_done=_opt_int(value.get("tasks_done")),
        max_tasks=_opt_int(value.get("max_tasks")),
        workers=list(value.get("workers") or []),
        last_seen=_opt_int(value.get("last_seen")),
        alive=bool(value.get("alive")),
        tasks=[_held(task) for task in value.get("tasks") or []],
    )


def _queue(value: dict) -> QueueStats:
    return QueueStats(
        name=value["name"],
        active=int(value.get("active") or 0),
        scheduled=int(value.get("scheduled") or 0),
        inflight=int(value.get("inflight") or 0),
        done=int(value.get("done") or 0),
        dead=int(value.get("dead") or 0),
        workers=int(value.get("workers") or 0),
        workers_alive=int(value.get("workers_alive") or 0),
    )


def _supervisor(value: dict) -> SupervisorInfo:
    return SupervisorInfo(
        host=value["host"],
        pid=int(value["pid"]),
        started_at=int(value.get("started_at") or 0),
        last_seen=int(value.get("last_seen") or 0),
        version=value.get("version") or "",
        queues=list(value.get("queues") or []),
        processes=int(value.get("processes") or 0),
        concurrency=int(value.get("concurrency") or 0),
        max_tasks=int(value.get("max_tasks") or 0),
        child_soft=_opt_int(value.get("child_soft")),
        child_hard=_opt_int(value.get("child_hard")),
        cgroup_current=_opt_int(value.get("cgroup_current")),
        cgroup_max=_opt_int(value.get("cgroup_max")),
        recycled=int(value.get("recycled") or 0),
        drains=int(value.get("drains") or 0),
        kills=int(value.get("kills") or 0),
        restarts=int(value.get("restarts") or 0),
        children=[_process(child) for child in value.get("children") or []],
    )


def _snapshot(value: dict) -> Snapshot:
    return Snapshot(
        taken_at=int(value.get("taken_at") or 0),
        queues=[_queue(item) for item in value.get("queues") or []],
        supervisors=[_supervisor(item) for item in value.get("supervisors") or []],
        orphans=[_process(item) for item in value.get("orphans") or []],
    )
