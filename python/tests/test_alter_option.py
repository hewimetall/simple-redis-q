"""The alter-option example: kwargs, headers, and a middleware class."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from simple_q import MemoryClient, actor


def _example():
    path = Path(__file__).resolve().parents[1] / "examples" / "alter_option.py"
    spec = importlib.util.spec_from_file_location("alter_option_example", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_alter_option_example():
    example = _example()
    middleware = asyncio.run(example.run())
    assert middleware.started
    assert middleware.stopped
    assert middleware.dead == []
    assert middleware.seen == example.EXPECTED


def test_alter_kwargs_survive_the_json_message_the_worker_hands_to_python():
    """The worker serializes the job to JSON before ``App.dispatch``."""

    example = _example()
    client = MemoryClient()
    middleware = example.AlterMiddleware()
    app = example.create_app(client, middleware)
    asyncio.run(client.send(example.recalc, project_id="42", alter=True))
    queued = client._queues["default"]
    queued[0] = json.loads(json.dumps(queued[0]))
    assert queued[0]["kwargs"]["alter"] is True
    asyncio.run(client.drain(app))
    assert middleware.seen == [example.EXPECTED[0]]


def test_actor_decorator_rejects_an_unknown_option():
    with pytest.raises(TypeError):
        actor(alter=True)


def test_a_non_string_header_is_rejected():
    example = _example()
    client = MemoryClient()
    with pytest.raises(TypeError):
        asyncio.run(
            client.send_with_options(
                example.recalc,
                kwargs={"project_id": "1", "alter": False},
                headers={"note": True},
            )
        )
