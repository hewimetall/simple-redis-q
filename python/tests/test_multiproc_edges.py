"""Multiprocess directory resolution, pid checks, and dead-file reaping."""

from __future__ import annotations

import os

from prometheus_client import CollectorRegistry

from simple_q.multiproc import (
    ReapingMultiProcessCollector,
    _slot_owner,
    pid_alive,
    prepare_multiproc_dir,
    reap_dead_processes,
    resolve_db_path,
)

_KEYS = (
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
)


def test_given_no_path_when_the_db_dir_is_resolved_then_env_then_temp_is_used(monkeypatch, tmp_path) -> None:
    # Arrange
    for key in _KEYS:
        monkeypatch.delenv(key, raising=False)

    # Act
    fallback = resolve_db_path(None)
    empty = resolve_db_path("")
    explicit = resolve_db_path(tmp_path)
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    from_env = resolve_db_path(None)

    # Assert
    assert fallback.endswith(os.path.join("dramatiq-prometheus"))
    assert empty == fallback or empty.endswith("dramatiq-prometheus")
    assert explicit == os.fspath(tmp_path)
    assert from_env == str(tmp_path)


def test_given_db_files_when_the_directory_is_cleaned_then_only_db_files_are_removed(tmp_path, monkeypatch) -> None:
    # Arrange
    for key in _KEYS:
        monkeypatch.setenv(key, os.environ.get(key, ""))
    (tmp_path / "a.db").write_bytes(b"a")
    (tmp_path / "note.txt").write_text("n", encoding="utf-8")

    # Act
    cleaned = prepare_multiproc_dir(tmp_path, clean=True)

    # Assert
    assert cleaned == str(tmp_path)
    assert not (tmp_path / "a.db").exists()
    assert (tmp_path / "note.txt").exists()
    assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == str(tmp_path)


def test_given_a_raced_remove_when_the_directory_is_cleaned_then_the_missing_file_is_ignored(
    tmp_path, monkeypatch
) -> None:
    # Arrange
    for key in _KEYS:
        monkeypatch.setenv(key, os.environ.get(key, ""))
    (tmp_path / "b.db").write_bytes(b"b")

    def remove(path: str) -> None:
        raise FileNotFoundError(path)

    monkeypatch.setattr(os, "remove", remove)

    # Act
    prepare_multiproc_dir(tmp_path, clean=True)

    # Assert
    assert (tmp_path / "b.db").exists()


def test_given_kill_errors_when_pid_alive_runs_then_each_error_has_its_own_answer(monkeypatch) -> None:
    # Arrange
    assert pid_alive(0) is False
    assert pid_alive(-1) is False

    def missing(pid: int, sig: int) -> None:
        raise ProcessLookupError

    def denied(pid: int, sig: int) -> None:
        raise PermissionError

    def other(pid: int, sig: int) -> None:
        raise OSError

    # Act
    monkeypatch.setattr(os, "kill", missing)
    gone = pid_alive(123456)
    monkeypatch.setattr(os, "kill", denied)
    allowed = pid_alive(123456)
    monkeypatch.setattr(os, "kill", other)
    failed = pid_alive(123456)

    # Assert
    assert gone is False
    assert allowed is True
    assert failed is False


def test_given_a_missing_directory_when_reap_runs_then_the_result_is_empty(tmp_path) -> None:
    # Arrange
    missing = tmp_path / "absent"

    # Act
    reaped = reap_dead_processes(missing)

    # Assert
    assert reaped == []


def test_given_an_unreadable_or_non_digit_slot_file_when_the_owner_is_read_then_it_is_none(tmp_path) -> None:
    # Arrange
    (tmp_path / "slot_7.pid").mkdir()
    (tmp_path / "slot_8.pid").write_text("nope", encoding="ascii")

    # Act
    unreadable = _slot_owner(str(tmp_path), 7)
    garbage = _slot_owner(str(tmp_path), 8)

    # Assert
    assert unreadable is None
    assert garbage is None


def test_given_our_pid_without_a_slot_file_when_reap_runs_then_the_gauge_stays(tmp_path, monkeypatch) -> None:
    # Arrange
    for key in _KEYS:
        monkeypatch.setenv(key, os.environ.get(key, ""))
    pid = os.getpid()
    (tmp_path / f"gauge_livesum_{pid}.db").write_bytes(b"live")

    # Act
    reaped = reap_dead_processes(tmp_path)

    # Assert
    assert reaped == []
    assert (tmp_path / f"gauge_livesum_{pid}.db").exists()


def test_given_a_dead_file_without_a_slot_record_when_reap_runs_then_the_missing_pid_file_is_ignored(
    tmp_path, monkeypatch
) -> None:
    # Arrange
    for key in _KEYS:
        monkeypatch.setenv(key, os.environ.get(key, ""))
    dead = 4_194_305
    (tmp_path / f"gauge_livesum_{dead}.db").write_bytes(b"stale")

    # Act
    reaped = reap_dead_processes(tmp_path)

    # Assert
    assert reaped == [dead]
    assert not (tmp_path / f"gauge_livesum_{dead}.db").exists()


def test_given_an_empty_directory_when_the_collector_runs_then_it_collects(tmp_path, monkeypatch) -> None:
    # Arrange
    for key in _KEYS:
        monkeypatch.setenv(key, os.environ.get(key, ""))
    registry = CollectorRegistry()

    # Act
    collector = ReapingMultiProcessCollector(registry, tmp_path)
    samples = list(collector.collect())

    # Assert
    assert collector._path == str(tmp_path)
    assert samples == []
