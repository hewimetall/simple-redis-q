"""Metrics HTTP server.

Any path returns the multiprocess scrape. Dead workers' live gauges are removed
first, matching the fixed Prometheus middleware. The parent process wipes
``*.db`` once; this server does not.
"""

from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, HTTPServer


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
            body = generate_latest(registry)
            self.send_response(200)
            self.send_header("content-type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    host, port = exposition_address()
    HTTPServer((host, port), Handler).serve_forever()
