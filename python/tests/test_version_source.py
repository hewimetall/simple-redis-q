"""workspace.package.version is the only crate version."""

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ("simple-q-runtime", "agent-worker")


def pep440(version: str) -> str:
    """Collapse Cargo's pre-release separator to the PEP 440 canonical form."""

    return re.sub(
        r"[-_.]?(alpha|a|beta|b|rc|c|preview|pre)[-_.]?(\d+)",
        lambda match: {
            "alpha": "a",
            "beta": "b",
            "c": "rc",
            "pre": "rc",
            "preview": "rc",
        }.get(match.group(1), match.group(1))
        + match.group(2),
        version,
        count=1,
    )


def test_workspace_package_version_is_the_only_source():
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())
    source = workspace["workspace"]["package"]["version"]
    assert source == "1.0.0-rc4"
    assert pep440(source) == "1.0.0rc4"

    for rel in ("crates/simple-q-runtime/Cargo.toml", "crates/agent-worker/Cargo.toml"):
        package = tomllib.loads((ROOT / rel).read_text())["package"]
        assert package["version"] == {"workspace": True}

    for rel in ("pyproject.toml", "crates/agent-worker/pyproject.toml"):
        project = tomllib.loads((ROOT / rel).read_text())["project"]
        assert "version" not in project
        assert "version" in project["dynamic"]
        assert "license" not in project

    worker = tomllib.loads((ROOT / "crates/agent-worker/pyproject.toml").read_text())
    assert worker["project"]["dependencies"] == [f"simple-q-runtime=={pep440(source)}"]

    runtime = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert runtime["tool"]["maturin"]["module-name"] == "simple_q._runtime"

    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    locked = {
        package["name"]: package["version"]
        for package in lock["package"]
        if package["name"] in PACKAGES
    }
    assert locked == {name: source for name in PACKAGES}

    uv_lock = tomllib.loads((ROOT / "uv.lock").read_text())
    uv_versions = {
        package["name"]: package["version"]
        for package in uv_lock["package"]
        if package["name"] == "simple-q-runtime"
    }
    assert uv_versions == {"simple-q-runtime": pep440(source)}

    marker = ROOT / "python" / "simple_q" / "py.typed"
    assert marker.is_file()
