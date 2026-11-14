"""``Tracing`` stores the exporter and fills targets only when they are omitted."""

from __future__ import annotations

from simple_q.tracing import DEFAULT_TARGETS, Tracing


class Exporter:
    def export(self, spans: object) -> None:
        return None

    def shutdown(self) -> None:
        return None


class Resource:
    pass


def test_given_no_targets_when_tracing_is_built_then_the_product_targets_are_used() -> None:
    # Arrange
    exporter = Exporter()
    resource = Resource()

    # Act
    tracing = Tracing(exporter=exporter, resource=resource)

    # Assert
    assert tracing.exporter is exporter
    assert tracing.resource is resource
    assert tracing.targets == DEFAULT_TARGETS
    assert tracing.targets["agent_worker"] == "info"
    assert tracing.max_queue_size == 2048
    assert tracing.max_export_batch_size == 512
    assert tracing.scheduled_delay_ms == 5000
    assert tracing.export_timeout_ms == 10000


def test_given_an_empty_target_dict_when_tracing_is_built_then_it_stays_empty() -> None:
    # Arrange
    targets: dict[str, str] = {}

    # Act
    tracing = Tracing(
        exporter=Exporter(),
        resource=Resource(),
        targets=targets,
        max_queue_size=1,
        max_export_batch_size=2,
        scheduled_delay_ms=3,
        export_timeout_ms=4,
    )

    # Assert
    assert tracing.targets == {}
    assert tracing.targets is not targets
    assert tracing.max_queue_size == 1
    assert tracing.max_export_batch_size == 2
    assert tracing.scheduled_delay_ms == 3
    assert tracing.export_timeout_ms == 4
