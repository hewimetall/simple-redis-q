"""Worker CLI and ``worker_alive`` across processes."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import textwrap
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _binary() -> Path:
    if os.environ.get("AGENT_WORKER_BIN"):
        return Path(os.environ["AGENT_WORKER_BIN"])
    debug = ROOT / "target" / "debug" / "agent-worker"
    release = ROOT / "target" / "release" / "agent-worker"
    if debug.is_file():
        return debug
    return release


def _worker_python(dest: Path) -> str:
    """Package tree for the embedded interpreter, without the wheel extension."""

    root = dest / "worker-py"
    package = root / "simple_q"
    if root.exists():
        shutil.rmtree(root)
    shutil.copytree(
        ROOT / "python" / "simple_q",
        package,
        ignore=shutil.ignore_patterns("*.so", "*.pyd", "__pycache__"),
    )
    return str(root)


def _redis_ok(url: str) -> bool:
    binary = _binary()
    if not binary.is_file():
        return False
    probe = subprocess.run(
        ["redis-cli", "-u", url, "PING"],
        capture_output=True,
        text=True,
        check=False,
    )
    return probe.returncode == 0 and "PONG" in probe.stdout


def test_worker_without_app_is_a_usage_error():
    binary = _binary()
    if not binary.is_file():
        pytest.skip("agent-worker is not built")
    result = subprocess.run(
        [str(binary), "worker"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "--app" in result.stderr


def test_factory_worker_alive_from_handler_and_another_process(tmp_path):
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
    if not _redis_ok(url):
        pytest.skip("redis is not reachable or agent-worker is not built")
    binary = _binary()
    queue = "aliveq"
    for key in (
        f"{queue}:data",
        f"{queue}:pending",
        f"{queue}:scheduled",
        f"{queue}:workers",
        f"{queue}:dead",
    ):
        subprocess.run(["redis-cli", "-u", url, "DEL", key], check=False, capture_output=True)

    app_path = tmp_path / "alive_app.py"
    result_path = tmp_path / "alive.json"
    app_path.write_text(
        textwrap.dedent(
            """
            import asyncio
            import json
            import os

            from simple_q import App, Client, Config, actor
            from simple_q.message import worker_name

            HOLDER = {}


            @actor(queue="aliveq", max_retries=0, time_limit=30_000)
            async def probe():
                client = HOLDER["client"]
                name = worker_name()
                progressed = {"ok": False}

                async def other():
                    await asyncio.sleep(0.01)
                    progressed["ok"] = True

                task = asyncio.create_task(other())
                alive = False
                saw = False
                for _ in range(40):
                    alive = await client.worker_alive(name)
                    if progressed["ok"]:
                        saw = True
                        break
                await task
                with open(os.environ["ALIVE_LOG"], "w", encoding="utf-8") as handle:
                    json.dump({"name": name, "alive": alive, "progressed": saw}, handle)

            def create_app():
                client = Client(os.environ["REDIS_URL"], config=Config.from_env())
                HOLDER["client"] = client
                return App(client=client, actors=[probe])
            """
        ),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": url,
            "PYTHONPATH": os.pathsep.join([str(tmp_path), _worker_python(tmp_path)]),
            "ALIVE_LOG": str(result_path),
            "RUST_LOG": "warn",
        }
    )
    other_env = env.copy()
    other_env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), str(ROOT / "python")])
    log = open(tmp_path / "worker.log", "ab")
    process = subprocess.Popen(
        [
            str(binary),
            "worker",
            "--app",
            "alive_app:create_app",
            "--factory",
            "--queues",
            queue,
            "--processes",
            "1",
            "--concurrency",
            "1",
        ],
        env=env,
        start_new_session=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        enqueued = subprocess.run(
            [str(binary), "enqueue", "--queue", queue, "--task", "probe", "--payload", "{}"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if enqueued.returncode != 0:
            worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
            raise AssertionError(f"{enqueued.stderr}\n{worker_log}")
        deadline = time.time() + 30
        while time.time() < deadline and not result_path.exists():
            if process.poll() is not None:
                break
            time.sleep(0.2)
        if not result_path.exists():
            worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
            raise AssertionError(f"handler did not record liveness\n{worker_log}")
        recorded = json.loads(result_path.read_text(encoding="utf-8"))
        assert recorded["alive"] is True
        assert recorded["progressed"] is True
        assert recorded["name"].startswith("agent-worker.aliveq.")

        script = tmp_path / "other_loop.py"
        script.write_text(
            textwrap.dedent(
                f"""
                import asyncio
                import json
                import os

                from simple_q import Client, Config, actor

                async def main():
                    client = Client(os.environ["REDIS_URL"], config=Config.from_env())

                    @actor(queue="otherq", max_retries=0)
                    async def marker(project_id: str) -> None:
                        return None

                    await client.send(marker, project_id="other")
                    progressed = {{"ok": False}}

                    async def other():
                        await asyncio.sleep(0.01)
                        progressed["ok"] = True

                    task = asyncio.create_task(other())
                    saw = False
                    alive = False
                    for _ in range(40):
                        alive = await client.worker_alive({recorded["name"]!r})
                        if progressed["ok"]:
                            saw = True
                            break
                    await task
                    await client.close()
                    print(json.dumps({{"alive": alive, "progressed": saw}}))

                asyncio.run(main())
                """
            ),
            encoding="utf-8",
        )
        other = subprocess.run(
            ["python3", str(script)],
            env=other_env,
            capture_output=True,
            text=True,
            check=False,
        )
        if other.returncode != 0:
            raise AssertionError(other.stderr or other.stdout)
        payload = json.loads(other.stdout.strip().splitlines()[-1])
        assert payload == {"alive": True, "progressed": True}
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        for key in (
            f"{queue}:data",
            f"{queue}:pending",
            f"{queue}:scheduled",
            f"{queue}:workers",
            f"{queue}:dead",
            "otherq:data",
            "otherq:pending",
            "otherq:scheduled",
        ):
            subprocess.run(["redis-cli", "-u", url, "DEL", key], check=False, capture_output=True)
