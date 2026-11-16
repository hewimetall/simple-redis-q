"""A worker slot becomes the Prometheus file id before metrics are created."""

from __future__ import annotations

import os

from prometheus_client import values

import simple_q.metrics as metrics
from simple_q.metrics import install_slot_identity, setup

_KEYS = (
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
    "SIMPLE_Q_SLOT",
)


def test_given_a_slot_and_a_prom_dir_when_setup_uses_multiprocess_then_the_slot_pid_is_written(
    tmp_path, monkeypatch
) -> None:
    # Arrange
    for key in _KEYS:
        if key in os.environ:
            monkeypatch.setenv(key, os.environ[key])
        else:
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SIMPLE_Q_SLOT", "3")
    monkeypatch.setenv("DRAMATIQ_PROM_DB", str(tmp_path))
    saved_ready = metrics._READY
    saved_metrics = dict(metrics._METRICS)
    previous_value = values.ValueClass
    metrics._READY = False
    try:
        # Act
        setup(multiprocess=True)
        install_slot_identity()

        # Assert
        assert (tmp_path / "slot_3.pid").read_text(encoding="ascii") == str(os.getpid())
        assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == str(tmp_path)
        assert values.ValueClass is not previous_value
        assert metrics._READY is True
        assert "total" in metrics._METRICS
    finally:
        metrics._READY = saved_ready
        metrics._METRICS.clear()
        metrics._METRICS.update(saved_metrics)
        values.ValueClass = previous_value


def test_given_no_multiprocess_flag_when_setup_runs_then_the_prom_env_selects_the_mode(
    tmp_path, monkeypatch
) -> None:
    # Arrange
    for key in _KEYS:
        if key in os.environ:
            monkeypatch.setenv(key, os.environ[key])
        else:
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    monkeypatch.delenv("SIMPLE_Q_SLOT", raising=False)
    monkeypatch.setenv("DRAMATIQ_PROM_DB", str(tmp_path))
    saved_ready = metrics._READY
    saved_metrics = dict(metrics._METRICS)
    previous_value = values.ValueClass
    metrics._READY = False
    try:
        # Act
        setup()

        # Assert
        assert metrics._READY is True
        assert "total" in metrics._METRICS
        assert (tmp_path / "slot_3.pid").exists() is False
    finally:
        metrics._READY = saved_ready
        metrics._METRICS.clear()
        metrics._METRICS.update(saved_metrics)
        values.ValueClass = previous_value
