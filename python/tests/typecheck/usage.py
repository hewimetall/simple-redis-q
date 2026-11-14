"""Calls that must type-check. Positional ``send`` is not one of them."""

from typing import Any

from opentelemetry.sdk.resources import Resource

from simple_q import App, MemoryClient, actor
from simple_q.dispatch import DeadLetter
from simple_q.interfaces import CallNext, Client, WorkerApp
from simple_q.tracing import Tracing


@actor
async def work(value: str) -> int:
    return len(value)


@actor(queue="default", max_retries=0)
async def other(left: int, right: str) -> None:
    return None


class Exporter:
    def export(self, spans: Any) -> Any:
        return None

    def shutdown(self) -> None:
        return None


def takes_client(client: Client) -> None:
    return None


def takes_app(app: WorkerApp) -> None:
    return None


class Layer:
    def install(self, client: Client, app: WorkerApp) -> None:
        client.on_send(self.on_send)
        app.on_start(self.on_start)
        app.on_dead(self.on_dead)
        app.around_dispatch(self.around)

    def on_send(
        self,
        actor_name: str,
        kwargs: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, str] | None:
        return headers

    async def on_start(self) -> None:
        return None

    def on_dead(self, letter: DeadLetter) -> None:
        return None

    async def around(self, message: dict[str, Any], call_next: CallNext) -> dict[str, Any]:
        return await call_next()


async def use(client: MemoryClient) -> str:
    takes_client(client)
    app = App(client=client, actors=[work, other])
    takes_app(app)
    Layer().install(client, app)
    traced = Tracing(
        exporter=Exporter(),
        resource=Resource.create({"service.name": "simple-q"}),
    )
    App(client=client, actors=[work], tracing=traced)
    produced = await work.function("ok")
    message_id = await client.send(work, value="ok")
    await client.send_with_options(other, kwargs={"left": 1, "right": "x"})
    await client.acquire_lock("gate", 30)
    await client.acquire_lock("gate", ttl=30)
    return message_id if produced == 2 else ""
