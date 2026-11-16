"""Prometheus exposition address, bad counter text, and the scrape server."""

from __future__ import annotations

import socket
import threading
import time
import urllib.error
import urllib.request
from http.server import HTTPServer

from prometheus_client import values

from simple_q.exposition import child_memory_samples, exposition_address, serve


_PROM_KEYS = (
    "DRAMATIQ_PROM_HOST",
    "dramatiq_prom_host",
    "DRAMATIQ_PROM_PORT",
    "dramatiq_prom_port",
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
    "SIMPLE_Q_SLOT",
)


def test_given_prom_env_when_the_address_is_read_then_uppercase_wins_over_the_default(monkeypatch) -> None:
    # Arrange
    for key in _PROM_KEYS:
        monkeypatch.delenv(key, raising=False)

    # Act
    default = exposition_address()
    monkeypatch.setenv("dramatiq_prom_host", "127.0.0.1")
    monkeypatch.setenv("dramatiq_prom_port", "9199")
    lowercase = exposition_address()
    monkeypatch.setenv("DRAMATIQ_PROM_HOST", "10.0.0.8")
    monkeypatch.setenv("DRAMATIQ_PROM_PORT", "9200")
    uppercase = exposition_address()

    # Assert
    assert default == ("0.0.0.0", 9191)
    assert lowercase == ("127.0.0.1", 9199)
    assert uppercase == ("10.0.0.8", 9200)


def test_given_a_bad_counter_when_samples_are_read_then_the_value_is_zero(tmp_path) -> None:
    # Arrange
    prom = tmp_path / "simple_q_child_memory.prom"
    samples = (
        "simple_q_child_memory_drains_total nope\n",
        "simple_q_child_memory_drains_total -1\n",
        "simple_q_child_memory_drains_total nan\n",
        "simple_q_child_memory_drains_total inf\n",
    )

    # Act
    texts = []
    for sample in samples:
        prom.write_text(sample, encoding="utf-8")
        texts.append(child_memory_samples(tmp_path).decode())

    # Assert
    for text in texts:
        assert "simple_q_child_memory_drains_total 0\n" in text
        assert "simple_q_child_memory_kills_total 0\n" in text


def test_given_a_free_port_when_serve_runs_then_a_get_returns_the_child_counters(tmp_path, monkeypatch) -> None:
    # Arrange
    for key in _PROM_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    monkeypatch.setenv("DRAMATIQ_PROM_HOST", "127.0.0.1")
    monkeypatch.setenv("DRAMATIQ_PROM_PORT", str(port))
    captured: dict[str, HTTPServer] = {}
    original = HTTPServer.serve_forever
    previous_value = values.ValueClass

    def tracking(self: HTTPServer, poll_interval: float = 0.5) -> None:
        captured["server"] = self
        original(self, poll_interval)

    monkeypatch.setattr(HTTPServer, "serve_forever", tracking)
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    body = b""
    content_type = ""
    status = 0
    try:
        # Act
        deadline = time.monotonic() + 5
        last: Exception | None = None
        while time.monotonic() < deadline:
            if "server" not in captured:
                time.sleep(0.01)
                continue
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as response:
                    status = response.status
                    content_type = response.headers["content-type"]
                    body = response.read()
                break
            except (urllib.error.URLError, ConnectionError, TimeoutError) as error:
                last = error
                time.sleep(0.05)
        else:
            raise AssertionError(last)

        # Assert
        assert status == 200
        assert "text/plain" in content_type
        assert b"simple_q_child_memory_drains_total" in body
        assert b"simple_q_child_memory_kills_total" in body
    finally:
        server = captured.get("server")
        if server is not None:
            server.shutdown()
            server.server_close()
        thread.join(timeout=5)
        values.ValueClass = previous_value
