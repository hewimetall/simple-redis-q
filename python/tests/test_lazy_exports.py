"""``Client``, ``Config``, and ``QueueAdmin`` load on first attribute access."""

from __future__ import annotations

import sys

import pytest


def test_given_unloaded_names_when_they_are_read_then_the_runtime_and_admin_types_load() -> None:
    # Arrange
    import simple_q

    module = sys.modules["simple_q"]
    for name in ("Client", "Config", "QueueAdmin"):
        module.__dict__.pop(name, None)

    try:
        # Act
        client = simple_q.Client
        config = simple_q.Config
        admin = simple_q.QueueAdmin

        # Assert
        assert client.__name__ == "Client"
        assert config.__name__ == "Config"
        assert admin.__name__ == "QueueAdmin"
        assert simple_q.Client is client
        with pytest.raises(AttributeError, match="no attribute 'missing_name'"):
            simple_q.missing_name
    finally:
        module.__dict__.pop("QueueAdmin", None)
