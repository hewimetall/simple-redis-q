from __future__ import annotations

import logging

from simple_q_runtime import (
    Controller as RuntimeController,
    acquire_lock,
    enqueue,
    release_lock,
)


class Task:
    """One registered controller method. This is the facade, not an Apalis API."""

    def __init__(
        self,
        function,
        *,
        entrypoint: bool,
        queue: str | None,
        lock_key: str | None,
        lock_ttl: int,
        skip_on_lock: bool,
        max_retries: int,
        next_hook: str | None,
        err_hook: str | None,
    ):
        self.function = function
        self.name = function.__name__
        self.entrypoint = entrypoint
        self.queue = queue
        self.lock_key = lock_key
        self.lock_ttl = lock_ttl
        self.skip_on_lock = skip_on_lock
        self.max_retries = max_retries
        self.next_hook = next_hook
        self.err_hook = err_hook

    def __set_name__(self, owner, name: str) -> None:
        self.name = name

    def __get__(self, controller, owner=None):
        if controller is None:
            return self
        return BoundTask(self, controller)

    async def invoke(self, controller, payload: dict):
        owner = None
        try:
            if self.lock_key is not None:
                owner = await acquire_lock(self.lock_key, self.lock_ttl)
                if owner is None:
                    if self.skip_on_lock:
                        return {"outcome": "skipped", "lock_key": self.lock_key}
                    raise RuntimeError(f"lock busy: {self.lock_key}")
            try:
                result = await self.function(controller, **payload)
            except Exception as error:
                await self._run_err(controller, payload, error)
                raise
            if self.next_hook is not None:
                await getattr(controller, self.next_hook)(self.name, payload, result)
            return result
        finally:
            await self._release(owner)

    async def _release(self, owner: str | None) -> None:
        if owner is None or self.lock_key is None:
            return
        try:
            await release_lock(self.lock_key, owner)
        except Exception:
            # The handler outcome is already decided. Renewal has stopped once
            # release runs, and the Redis key still expires by its TTL.
            logging.getLogger(__name__).warning(
                "lock release failed for %s; the task result stands and the key expires by ttl",
                self.lock_key,
                exc_info=True,
            )

    async def _run_err(self, controller, payload: dict, error: Exception) -> None:
        if self.err_hook is None:
            return
        try:
            await getattr(controller, self.err_hook)(self.name, payload, error)
        except Exception:
            return


class BoundTask:
    def __init__(self, task: Task, controller):
        self.task = task
        self.controller = controller

    async def __call__(self, **payload):
        if not self.task.entrypoint:
            raise RuntimeError(f"{self.task.name} is not an entrypoint")
        return await self.task.invoke(self.controller, payload)

    async def send(self, **payload):
        import json

        queue = self.task.queue or self.controller.queue
        await enqueue(queue, self.task.name, json.dumps(payload))


def task(
    *,
    entrypoint: bool = False,
    queue: str | None = None,
    lock_key: str | None = None,
    lock_ttl: int = 30,
    skip_on_lock: bool = False,
    max_retries: int = 0,
    next: str | None,
    err: str | None,
):
    """Register a controller method.

    ``next`` and ``err`` are required. Pass ``None`` when the hook is absent.
    ``max_retries`` other than 0 is rejected: apalis-redis 1.0.0 reschedules a
    Failed result while ``attempt < 25`` and does not read ``max_attempts``.
    """

    if max_retries != 0:
        raise ValueError(
            "only max_retries=0 is supported until the Redis backend honors max_attempts"
        )
    if lock_ttl <= 0:
        raise ValueError("lock_ttl must be greater than zero")
    if lock_key is not None and (
        not lock_key or lock_key.startswith("lock:") or any(char.isspace() for char in lock_key)
    ):
        raise ValueError(
            "lock_key must be a non-empty server key such as 'cache:discovery' without the lock: prefix"
        )

    def decorate(function):
        return Task(
            function,
            entrypoint=entrypoint,
            queue=queue,
            lock_key=lock_key,
            lock_ttl=lock_ttl,
            skip_on_lock=skip_on_lock,
            max_retries=max_retries,
            next_hook=next,
            err_hook=err,
        )

    return decorate


class Controller(RuntimeController):
    """Controller connected to the embedded runtime.

    The queue on the class is the default. A task may override it.
    """

    queue = "control"

    def __init__(self):
        super().__init__()
        tasks: dict[str, Task] = {}
        for cls in reversed(type(self).mro()):
            for name, value in vars(cls).items():
                if isinstance(value, Task):
                    tasks[name] = value
        for name, spec in tasks.items():
            for hook in (spec.next_hook, spec.err_hook):
                if hook is not None and not hasattr(self, hook):
                    raise AttributeError(f"{type(self).__name__}.{name} names missing hook {hook}")
        self._tasks = tasks

    async def execute(self, task_name: str, payload: dict):
        spec = self._tasks.get(task_name)
        if spec is None:
            raise KeyError(task_name)
        return await spec.invoke(self, payload or {})
