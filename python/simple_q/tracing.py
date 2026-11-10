"""Tracing configuration handed to the Rust child.

The worker has no OTLP client. ``Tracing`` names the Python exporter that
receives finished spans, the OpenTelemetry resource attached to them, and the
per-target levels for the OpenTelemetry layer.
"""

from __future__ import annotations

from typing import Any


class Tracing:
    def __init__(
        self,
        *,
        exporter: Any,
        resource: Any,
        targets: dict[str, str] | None = None,
        max_queue_size: int = 2048,
        max_export_batch_size: int = 512,
        scheduled_delay_ms: int = 5000,
        export_timeout_ms: int = 10000,
    ):
        self.exporter = exporter
        self.resource = resource
        self.targets = dict(targets or {})
        self.max_queue_size = max_queue_size
        self.max_export_batch_size = max_export_batch_size
        self.scheduled_delay_ms = scheduled_delay_ms
        self.export_timeout_ms = export_timeout_ms
