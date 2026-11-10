"""Metrics HTTP server.

Any path returns the multiprocess scrape. Dead workers' live gauges are removed
first, matching the fixed Prometheus middleware. The parent process wipes
``*.db`` once; this server does not.

The supervisor is not this process. It writes
``simple_q_child_memory_drains_total`` and
``simple_q_child_memory_kills_total`` into ``simple_q_child_memory.prom`` in
the same directory. The scrape adds those two samples. A missing file is zero.
"""

from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, HTTPServer

_CHILD_MEMORY_PROM = "simple_q_child_memory.prom"
_CHILD_MEMORY_COUNTERS = (
    (
        "simple_q_child_memory_drains_total",
        "Children drained because a memory limit was crossed.",
    ),
    (
        "simple_q_child_memory_kills_total",
        "Children killed because a memory limit was crossed.",
    ),
)


def exposition_address() -> tuple[str, int]:
    host = (
        os.environ.get("DRAMATIQ_PROM_HOST")
        or os.environ.get("dramatiq_prom_host")
        or "0.0.0.0"
    )
    port = (
        os.environ.get("DRAMATIQ_PROM_PORT")
        or os.environ.get("dramatiq_prom_port")
        or "9191"
    )
    return host, int(port)


def _counter_value(text: str, name: str) -> int:
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2 or parts[0] != name:
            continue
        try:
            value = float(parts[1])
        except ValueError:
            return 0
        if value < 0 or value != value or value == float("inf"):
            return 0
        return int(value)
    return 0


def child_memory_samples(path: str | os.PathLike[str] | None = None) -> bytes:
    """Prometheus text for the supervisor's child-memory counters.

    A missing or unreadable file yields zero for both samples. That is not
    an error: the metrics process starts before any drain or kill.
    """

    from simple_q.multiproc import resolve_db_path

    file_path = os.path.join(resolve_db_path(path), _CHILD_MEMORY_PROM)
    try:
        with open(file_path, encoding="utf-8") as handle:
            text = handle.read()
    except (OSError, UnicodeError):
        text = ""
    lines: list[str] = []
    for name, help_text in _CHILD_MEMORY_COUNTERS:
        value = _counter_value(text, name)
        lines.append(f"# HELP {name} {help_text}\n")
        lines.append(f"# TYPE {name} counter\n")
        lines.append(f"{name} {value}\n")
    return "".join(lines).encode("utf-8")


def serve() -> None:
    from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

    from simple_q.metrics import install_slot_identity
    from simple_q.multiproc import ReapingMultiProcessCollector, prepare_multiproc_dir

    prepare_multiproc_dir(clean=False)
    install_slot_identity()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            registry = CollectorRegistry()
            ReapingMultiProcessCollector(registry)
            body = generate_latest(registry) + child_memory_samples()
            self.send_response(200)
            self.send_header("content-type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    host, port = exposition_address()
    HTTPServer((host, port), Handler).serve_forever()
