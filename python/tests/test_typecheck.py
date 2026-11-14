"""Lock the client annotations against pyright 1.1.414.

``**kwargs: P.kwargs`` without ``*args: P.args`` is rejected by the checker and
does not validate keyword names. ``send`` stays keyword-only, so positional
calls are errors. ``Actor.function`` keeps the handler parameters.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _pyright(*paths: str) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("pyright")
    if binary is None:
        pytest.skip("pyright is not installed")
    return subprocess.run(
        [binary, *paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_package_types() -> None:
    result = _pyright()
    assert result.returncode == 0, result.stdout + result.stderr


def test_kwargs_component_alone_is_rejected() -> None:
    result = _pyright("python/tests/typecheck/rejected_kwargs_component.py")
    assert result.returncode != 0, result.stdout + result.stderr


def test_positional_send_is_rejected() -> None:
    result = _pyright("python/tests/typecheck/rejected_positional_send.py")
    combined = result.stdout + result.stderr
    assert result.returncode != 0, combined
    assert "positional" in combined.lower() or "argument" in combined.lower()


def test_handler_argument_is_checked() -> None:
    result = _pyright("python/tests/typecheck/rejected_handler_arg.py")
    combined = result.stdout + result.stderr
    assert result.returncode != 0, combined
    assert "str" in combined
