import asyncio

from simple_q_runtime import Controller


class MyController(Controller):
    async def execute(self, task_name: str, payload: dict):
        print(f"execute: {task_name}, {payload}", flush=True)
        await asyncio.sleep(0.1)
        return {"ok": True}

    async def next(self, task_name: str, payload: dict, result):
        print(f"next: {result}", flush=True)

    async def err(self, task_name: str, payload: dict, error):
        print(f"err: {error!r}", flush=True)


app = MyController()