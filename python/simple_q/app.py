import asyncio

from simple_q import Controller, task


class App(Controller):
    queue = "control"

    @task(
        entrypoint=True,
        lock_key=None,
        lock_ttl=30,
        skip_on_lock=True,
        max_retries=0,
        next="after_demo",
        err="on_demo_error",
    )
    async def demo(self, trace_ctx: dict | None = None, task_id: str | None = None):
        print(f"execute: demo, task_id={task_id}, trace_ctx={trace_ctx}", flush=True)
        await asyncio.sleep(0.1)
        return {"ok": True, "task_id": task_id}

    async def after_demo(self, task_name: str, payload: dict, result):
        print(f"next: {task_name} {result}", flush=True)

    async def on_demo_error(self, task_name: str, payload: dict, error):
        print(f"err: {task_name} {error!r}", flush=True)

    @task(
        entrypoint=False,
        lock_key="demo:gate",
        lock_ttl=30,
        skip_on_lock=True,
        max_retries=0,
        next=None,
        err="on_demo_error",
    )
    async def gated(self, task_id: str | None = None):
        print(f"gated-ran {task_id}", flush=True)
        return {"ok": True}

    @task(
        entrypoint=False,
        lock_key=None,
        lock_ttl=30,
        skip_on_lock=False,
        max_retries=0,
        next=None,
        err="on_demo_error",
    )
    async def boom(self, task_id: str | None = None):
        raise RuntimeError(f"boom {task_id}")


app = App()
