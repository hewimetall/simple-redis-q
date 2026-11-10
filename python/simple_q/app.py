import asyncio

from simple_q import Registry

app = Registry()


@app.actor(queue="default", max_retries=0, name="demo")
async def demo(trace_ctx: dict | None = None, task_id: str | None = None):
    print(f"execute: demo, task_id={task_id}, trace_ctx={trace_ctx}", flush=True)
    await asyncio.sleep(0.1)
    return {"ok": True, "task_id": task_id}


@app.actor(
    queue="default",
    max_retries=0,
    lock_key="demo:gate",
    lock_ttl=30,
    skip_on_lock=True,
    name="gated",
)
async def gated(task_id: str | None = None):
    print(f"gated-ran {task_id}", flush=True)
    return {"ok": True}


@app.actor(queue="quota", max_retries=0, name="boom")
async def boom(task_id: str | None = None):
    raise RuntimeError(f"boom {task_id}")
