"""``agent-worker info`` and ``QueueAdmin.snapshot`` see the same process."""

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


def _stable(payload: dict) -> dict:
    def walk(value):
        if isinstance(value, dict):
            return {
                key: walk(item)
                for key, item in value.items()
                if key not in {"taken_at", "last_seen", "rss", "cgroup_current", "cgroup_max"}
            }
        if isinstance(value, list):
            return [walk(item) for item in value]
        return value

    return walk(payload)


def test_info_without_app_is_a_usage_error(monkeypatch):
    binary = _binary()
    if not binary.is_file():
        pytest.skip("agent-worker is not built")
    monkeypatch.delenv("SIMPLE_Q_APP", raising=False)
    result = subprocess.run(
        [str(binary), "info"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "--app" in result.stderr


def test_info_json_shows_the_running_task_then_max_tasks(tmp_path):
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
    if not _redis_ok(url):
        pytest.skip("redis is not reachable or agent-worker is not built")
    binary = _binary()
    queue = "infoq"
    host = f"infohost{os.getpid()}"
    for key in (
        f"{queue}:data",
        f"{queue}:active",
        f"{queue}:scheduled",
        f"{queue}:workers",
        f"{queue}:done",
        f"{queue}:dead",
        f"{queue}:signal",
    ):
        subprocess.run(["redis-cli", "-u", url, "DEL", key], check=False, capture_output=True)

    app_path = tmp_path / "info_app.py"
    app_path.write_text(
        textwrap.dedent(
            """
            import asyncio
            import os

            from simple_q import App, Client, Config, actor


            @actor(queue="infoq", max_retries=0, time_limit=30_000)
            async def hold():
                await asyncio.sleep(4)


            def create():
                return App(
                    client=Client(os.environ["REDIS_URL"], config=Config.from_env()),
                    actors=[hold],
                )
            """
        ),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": url,
            "PYTHONPATH": os.pathsep.join([str(tmp_path), _worker_python(tmp_path)]),
            "HOSTNAME": host,
            "SIMPLE_Q_HEARTBEAT_SECS": "1",
            "SIMPLE_Q_MISSED_HEARTBEATS": "20",
            "SIMPLE_Q_MEMORY_SOFT_RATIO": "0.99",
            "SIMPLE_Q_MEMORY_HARD_RATIO": "0.999",
            "RUST_LOG": "warn",
            "SIMPLE_Q_APP": "info_app:create",
            "SIMPLE_Q_APP_FACTORY": "1",
        }
    )
    log = open(tmp_path / "worker.log", "ab")
    process = subprocess.Popen(
        [
            str(binary),
            "worker",
            "--queues",
            queue,
            "--processes",
            "1",
            "--max-tasks-per-process",
            "1",
        ],
        env=env,
        start_new_session=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )

    def info_json() -> dict:
        result = subprocess.run(
            [str(binary), "info", "--queue", queue, "--json"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 and not result.stdout.strip():
            worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
            raise AssertionError(f"{result.stderr}\n{worker_log}")
        if not result.stdout.strip():
            return {}
        return json.loads(result.stdout)

    try:
        enqueued = subprocess.run(
            [str(binary), "enqueue", "--queue", queue, "--task", "hold", "--payload", "{}"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if enqueued.returncode != 0:
            worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
            raise AssertionError(f"{enqueued.stderr}\n{worker_log}")

        running = None
        deadline = time.time() + 25
        while time.time() < deadline:
            if process.poll() is not None:
                break
            snap = info_json()
            for supervisor in snap.get("supervisors") or []:
                for child in supervisor.get("children") or []:
                    actors = [task.get("actor_name") for task in child.get("tasks") or []]
                    if child.get("state") == "running" and "hold" in actors:
                        running = (child.get("pid"), snap)
                        break
                if running:
                    break
            if running:
                break
            time.sleep(0.4)
        if running is None:
            worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
            raise AssertionError(f"info did not show the running task\n{worker_log}")
        running_pid, snap = running

        try:
            from simple_q._runtime import Admin as _Admin
            from simple_q.admin import QueueAdmin
            from simple_q import Client, Config
        except Exception:
            _Admin = None
        if _Admin is not None:
            import asyncio
            from dataclasses import asdict

            os.environ["SIMPLE_Q_HEARTBEAT_SECS"] = "1"
            os.environ["SIMPLE_Q_MISSED_HEARTBEATS"] = "20"
            os.environ["SIMPLE_Q_MEMORY_SOFT_RATIO"] = "0.99"
            os.environ["SIMPLE_Q_MEMORY_HARD_RATIO"] = "0.999"

            async def grab():
                client = Client(url, Config.from_env())
                admin = QueueAdmin(client)
                found = await admin.snapshot([queue], host=host)
                await client.close()
                return found

            compared = asyncio.run(grab())
            assert _stable(asdict(compared)) == _stable(snap)

        replaced = False
        deadline = time.time() + 40
        while time.time() < deadline:
            snap = info_json()
            children = [
                child
                for supervisor in snap.get("supervisors") or []
                for child in supervisor.get("children") or []
            ]
            exited = [
                child
                for child in children
                if child.get("state") == "exited" and child.get("reason") == "max_tasks"
            ]
            live = [
                child
                for child in children
                if child.get("ended_at") is None and child.get("pid") != running_pid
            ]
            if exited and live:
                replaced = True
                break
            time.sleep(0.4)
        if not replaced:
            worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
            raise AssertionError(f"info did not show exited/max_tasks and a new pid\n{worker_log}")
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        listed = subprocess.run(
            ["redis-cli", "-u", url, "ZRANGE", "simple_q:supervisors", "0", "-1"],
            capture_output=True,
            text=True,
            check=False,
        )
        for member in listed.stdout.split():
            if member.startswith(f"{host}."):
                subprocess.run(
                    ["redis-cli", "-u", url, "ZREM", "simple_q:supervisors", member],
                    check=False,
                    capture_output=True,
                )
                subprocess.run(
                    ["redis-cli", "-u", url, "DEL", f"simple_q:supervisor:{member}"],
                    check=False,
                    capture_output=True,
                )
