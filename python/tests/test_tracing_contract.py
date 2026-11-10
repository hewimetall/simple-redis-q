"""Rust process spans are children of the sender trace and keep their links.

The worker binary and Redis are required. OpenTelemetry is required only to
construct the Resource the child checks; the exporter itself is a fake.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import textwrap
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BINARY = Path(os.environ.get("AGENT_WORKER_BIN", ROOT / "target" / "release" / "agent-worker"))

SENDER = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
LINK = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-b7ad6b7169203331-01"
SENDER_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
SENDER_SPAN = "00f067aa0ba902b7"
LINK_SPAN = "b7ad6b7169203331"


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


def test_process_span_is_a_child_and_keeps_its_link(tmp_path):
    pytest.importorskip("opentelemetry.sdk.resources")
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
    if not _redis_ok(url):
        pytest.skip("redis is not reachable or agent-worker is not built")

    span_log = tmp_path / "spans.jsonl"
    header_log = tmp_path / "headers.jsonl"
    app_path = tmp_path / "trace_app.py"
    app_path.write_text(
        textwrap.dedent(
            """
            import json
            import os

            from opentelemetry.sdk.resources import Resource
            from opentelemetry.trace import SpanKind

            from simple_q import Registry, Tracing
            from simple_q.message import current_message

            RESOURCE = Resource.create({"service.name": "agent-worker"})


            class Recorder:
                def __init__(self, path, resource):
                    self.path = path
                    self.resource = resource

                def export(self, spans):
                    with open(self.path, "a", encoding="utf-8") as handle:
                        for span in spans:
                            context = span.context
                            parent = span.parent
                            links = []
                            for link in list(span.links or []):
                                link_context = getattr(link, "context", link)
                                links.append(
                                    {
                                        "trace_id": format(int(link_context.trace_id), "032x"),
                                        "span_id": format(int(link_context.span_id), "016x"),
                                    }
                                )
                            parent_context = getattr(parent, "context", parent) if parent is not None else None
                            handle.write(
                                json.dumps(
                                    {
                                        "name": span.name,
                                        "trace_id": format(int(context.trace_id), "032x"),
                                        "span_id": format(int(context.span_id), "016x"),
                                        "parent_span_id": (
                                            None
                                            if parent_context is None
                                            else format(int(parent_context.span_id), "016x")
                                        ),
                                        "links": links,
                                        "kind": "consumer" if span.kind == SpanKind.CONSUMER else str(span.kind),
                                        "same_resource": span.resource is self.resource,
                                        "message_id": (span.attributes or {}).get("messaging.message.id"),
                                    }
                                )
                                + "\\n"
                            )
                        handle.flush()

                def shutdown(self):
                    return None


            app = Registry(
                tracing=Tracing(
                    exporter=Recorder(os.environ["SPAN_LOG"], RESOURCE),
                    resource=RESOURCE,
                    targets={
                        "agent_worker": "info",
                        "simple_q_runtime": "info",
                        "apalis": "debug",
                    },
                    max_queue_size=2048,
                    max_export_batch_size=512,
                    scheduled_delay_ms=200,
                    export_timeout_ms=10000,
                )
            )


            @app.actor(
                name="traced",
                queue="traceq",
                max_retries=1,
                time_limit=30_000,
                min_backoff=1,
                max_backoff=1000,
            )
            async def traced():
                current = current_message()
                with open(os.environ["HEADER_LOG"], "a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(
                            {
                                "attempt": current.retries,
                                "traceparent": current.headers.get("traceparent"),
                                "tracelink": current.headers.get("tracelink"),
                            }
                        )
                        + "\\n"
                    )
                if current.retries == 0:
                    raise RuntimeError("again")
                return {"ok": True}
            """
        ),
        encoding="utf-8",
    )

    queue = "traceq"
    _flush(url, queue)
    env = os.environ.copy()
    user_site = ""
    try:
        import site

        user_site = site.getusersitepackages()
    except Exception:
        user_site = ""
    env.update(
        {
            "REDIS_URL": url,
            "PYTHONPATH": os.pathsep.join(
                part for part in (str(tmp_path), str(ROOT / "python"), user_site) if part
            ),
            "SPAN_LOG": str(span_log),
            "HEADER_LOG": str(header_log),
            "RUST_LOG": "warn",
        }
    )
    log = open(tmp_path / "worker.log", "ab")
    process = subprocess.Popen(
        [
            str(BINARY),
            "worker",
            "--app",
            "trace_app:app",
            "--queues",
            queue,
            "--processes",
            "1",
            "--concurrency",
            "1",
            "--max-tasks-per-process",
            "10",
        ],
        env=env,
        start_new_session=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        headers = json.dumps({"traceparent": SENDER, "tracelink": LINK})
        enqueued = subprocess.run(
            [
                str(BINARY),
                "enqueue",
                "--queue",
                queue,
                "--task",
                "traced",
                    "--payload",
                    "{}",
                    "--headers",
                    headers,
                    "--max-retries",
                    "1",
                    "--min-backoff-ms",
                    "1",
                    "--max-backoff-ms",
                    "1000",
                ],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if enqueued.returncode != 0:
            raise AssertionError(enqueued.stderr or enqueued.stdout)
        attempts = _wait_for_lines(header_log, 2, 30)
        assert [row["attempt"] for row in attempts] == [0, 1]
        assert all(row["tracelink"] == LINK for row in attempts)
        assert all(row["traceparent"] != SENDER for row in attempts)
        assert attempts[0]["traceparent"] != attempts[1]["traceparent"]
    finally:
        _stop(process)

    if not span_log.exists():
        worker_log = (tmp_path / "worker.log").read_text(encoding="utf-8", errors="replace")
        raise AssertionError(f"no spans were exported\n{worker_log}")
    spans = [
        row
        for row in _wait_for_lines(span_log, 1, 1)
        if str(row.get("name", "")).startswith("process ")
    ]
    by_id = {row["span_id"]: row for row in spans}
    assert len(by_id) >= 2, spans
    for row in by_id.values():
        assert row["trace_id"] == SENDER_TRACE
        assert row["parent_span_id"] == SENDER_SPAN
        assert row["kind"] == "consumer"
        assert row["same_resource"] is True
        assert any(link["span_id"] == LINK_SPAN for link in row["links"])
    for attempt in attempts:
        parts = attempt["traceparent"].split("-")
        assert parts[0] == "00"
        assert parts[1] == SENDER_TRACE
        assert parts[2] in by_id
        assert parts[2] != SENDER_SPAN


def _flush(url: str, queue: str) -> None:
    keys = subprocess.run(
        ["redis-cli", "-u", url, "--scan", "--pattern", f"{queue}*"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    for key in keys:
        subprocess.run(["redis-cli", "-u", url, "DEL", key], check=False, capture_output=True)


def _wait_for_lines(path: Path, count: int, timeout: float) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and path.read_text(encoding="utf-8").strip():
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
            if len(rows) >= count:
                return rows
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {path}")


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
