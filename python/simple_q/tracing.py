"""Tracing configuration handed to the Rust child.

The worker has no OTLP client. ``Tracing`` names the Python exporter that
receives finished spans, the OpenTelemetry resource attached to them, and the
per-target levels for the OpenTelemetry layer.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import ReadableSpan
    from opentelemetry.sdk.trace.export import SpanExportResult

DEFAULT_TARGETS = {
    "agent_worker": "info",
    "simple_q_runtime": "info",
    "apalis": "debug",
}


class SpanExporter(Protocol):
    """The two methods the worker calls. A real OpenTelemetry exporter matches."""

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult: ...

    def shutdown(self) -> None: ...


class Tracing:
    def __init__(
        self,
        *,
        exporter: SpanExporter,
        resource: Resource,
        targets: dict[str, str] | None = None,
        max_queue_size: int = 2048,
        max_export_batch_size: int = 512,
        scheduled_delay_ms: int = 5000,
        export_timeout_ms: int = 10000,
    ) -> None:
        self.exporter: SpanExporter = exporter
        self.resource: Resource = resource
        # None uses the product crates. An explicit empty dict stays empty
        # and the child rejects it, because an empty filter would export nothing.
        self.targets = dict(DEFAULT_TARGETS if targets is None else targets)
        self.max_queue_size = max_queue_size
        self.max_export_batch_size = max_export_batch_size
        self.scheduled_delay_ms = scheduled_delay_ms
        self.export_timeout_ms = export_timeout_ms
