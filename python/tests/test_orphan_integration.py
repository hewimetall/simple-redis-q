"""Redis integration checks for lease renewal and orphan redelivery.

The production orphan window is 60 seconds. These tests shorten the heartbeat
so a task can still outlive the window without waiting ten minutes. Redis 8 is
the second URL when REDIS_URL_8 is set.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BINARY = Path(os.environ.get("AGENT_WORKER_BIN", ROOT / "target" / "release" / "agent-worker"))
HEARTBEAT_SECS = 5
MISSED_HEARTBEATS = 2
ORPHAN_SECS = HEARTBEAT_SECS * MISSED_HEARTBEATS


def _redis_urls():
    primary = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
    urls = [("redis7", primary)]
    secondary = os.environ.get("REDIS_URL_8")
    if secondary:
        urls.append(("redis8", secondary))
    return urls


def _redis_ok(url: str) -> bool:
    if not BINARY.is_file():
        return False
    probe = subprocess.run(
        ["redis-cli", "-u", url, "PING"],
        capture_output=True,
        text=True,
        check=False,
    )
    return probe.returncode == 0 and "PONG" in probe.stdout


def _flush(url: str, queue: str) -> None:
    keys = subprocess.run(
        ["redis-cli", "-u", url, "--scan", "--pattern", f"{queue}*"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    delivery = subprocess.run(
        ["redis-cli", "-u", url, "--scan", "--pattern", "delivery:*"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    for key in keys + delivery:
        subprocess.run(["redis-cli", "-u", url, "DEL", key], check=False, capture_output=True)


def _worker_env(url: str, extra: dict[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": url,
            "PYTHONPATH": str(ROOT / "python"),
            "SIMPLE_Q_HEARTBEAT_SECS": str(HEARTBEAT_SECS),
            "SIMPLE_Q_MISSED_HEARTBEATS": str(MISSED_HEARTBEATS),
            "SIMPLE_Q_MAX_DELIVERIES": extra.get("SIMPLE_Q_MAX_DELIVERIES", "5"),
        }
    )
    env.update(extra)
    return env


def _start_worker(url: str, queue: str, extra: dict[str, str]) -> subprocess.Popen:
    log = open("/tmp/agent-worker-orphan.log", "ab")
    return subprocess.Popen(
        [
            str(BINARY),
            "worker",
            "--app",
            "integration_app:app",
            "--queues",
            queue,
            "--processes",
            "2",
            "--concurrency",
            "1",
            "--max-tasks-per-process",
            "50",
        ],
        env=_worker_env(url, extra),
        start_new_session=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def _enqueue(url: str, queue: str, task: str, extra: dict[str, str] | None = None) -> None:
    subprocess.run(
        [str(BINARY), "enqueue", "--queue", queue, "--task", task, "--payload", "{}"],
        env=_worker_env(url, extra or {}),
        check=True,
        capture_output=True,
        text=True,
    )


def _wait_for_lines(path: Path, count: int, timeout: float) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and path.read_text(encoding="utf-8").strip():
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
            if len(rows) >= count:
                return rows
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {path}")


def _scores(url: str, queue: str) -> dict[str, float]:
    raw = subprocess.run(
        ["redis-cli", "-u", url, "ZRANGE", f"{queue}:workers", "0", "-1", "WITHSCORES"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return {raw[index]: float(raw[index + 1]) for index in range(0, len(raw), 2)}


def _locked_at(url: str, queue: str) -> list[int]:
    keys = subprocess.run(
        ["redis-cli", "-u", url, "--scan", "--pattern", f"{queue}:meta:*"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    values = []
    for key in keys:
        raw = subprocess.run(
            ["redis-cli", "-u", url, "HGET", key, "locked_at"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if raw:
            values.append(int(raw))
    return values


@pytest.mark.parametrize("label,url", _redis_urls())
def test_long_task_keeps_its_lease_past_the_orphan_window(label, url, tmp_path):
    if not _redis_ok(url):
        pytest.skip(f"{label} is not reachable or agent-worker is not built")
    queue = "holdq"
    _flush(url, queue)
    touch = tmp_path / "hold.jsonl"
    process = _start_worker(
        url,
        queue,
        {"HOLD_TOUCH": str(touch), "HOLD_SECONDS": str(ORPHAN_SECS + 15), "DEAD_LOG": str(tmp_path / "dead.jsonl")},
    )
    try:
        _enqueue(url, queue, "hold")
        _wait_for_lines(touch, 1, 30)
        first_scores = _scores(url, queue)
        first_locks = _locked_at(url, queue)
        assert first_scores, "workers did not register"
        assert first_locks, "locked_at was not recorded"
        time.sleep(HEARTBEAT_SECS + 2)
        second_scores = _scores(url, queue)
        second_locks = _locked_at(url, queue)
        assert max(second_scores.values()) > max(first_scores.values())
        assert max(second_locks) > max(first_locks)
        assert len(_wait_for_lines(touch, 1, 1)) == 1
        dead = subprocess.run(
            ["redis-cli", "-u", url, "ZCARD", f"{queue}:dead"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert dead == "0"
        time.sleep(ORPHAN_SECS)
        assert len(_wait_for_lines(touch, 1, 1)) == 1
    finally:
        _stop(process)
        _flush(url, queue)


@pytest.mark.parametrize("label,url", _redis_urls())
def test_sigkill_redelivers_and_then_poison_calls_on_dead(label, url, tmp_path):
    if not _redis_ok(url):
        pytest.skip(f"{label} is not reachable or agent-worker is not built")
    queue = "killq"
    _flush(url, queue)
    touch = tmp_path / "kill.jsonl"
    dead_log = tmp_path / "dead.jsonl"
    process = _start_worker(
        url,
        queue,
        {
            "KILL_TOUCH": str(touch),
            "DEAD_LOG": str(dead_log),
            "SIMPLE_Q_MAX_DELIVERIES": "1",
        },
    )
    try:
        _enqueue(url, queue, "die", {"SIMPLE_Q_MAX_DELIVERIES": "1"})
        first = _wait_for_lines(touch, 1, 30)[0]
        os.kill(int(first["pid"]), signal.SIGKILL)
        deadline = time.time() + ORPHAN_SECS + 30
        while time.time() < deadline:
            if dead_log.exists() and dead_log.read_text(encoding="utf-8").strip():
                break
            time.sleep(0.5)
        else:
            raise AssertionError("poison hook did not run after the child was killed")
        rows = [json.loads(line) for line in dead_log.read_text(encoding="utf-8").splitlines() if line]
        assert len(rows) == 1
        assert rows[0]["actor_name"] == "die"
        assert rows[0]["reason"] == "poison"
        assert rows[0]["retries"] == 0
        assert rows[0]["message_id"]
        message_id = rows[0]["message_id"]
        deliveries = subprocess.run(
            ["redis-cli", "-u", url, "GET", f"delivery:{message_id}:0"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert int(deliveries) >= 2
    finally:
        _stop(process)
        _flush(url, queue)
