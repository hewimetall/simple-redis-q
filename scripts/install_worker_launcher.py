#!/usr/bin/env python3
"""Replace the wheel's agent-worker ELF with a launcher plus agent-worker.bin."""

from __future__ import annotations

import base64
import hashlib
import sys
import zipfile
from pathlib import Path


def _record_row(name: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{name},sha256={digest},{len(data)}"


def rewrite_wheel(wheel: Path, launcher: bytes) -> None:
    """Put ``launcher`` at ``agent-worker`` and keep the ELF as ``agent-worker.bin``."""

    with zipfile.ZipFile(wheel) as archive:
        items = {
            info.filename: archive.read(info.filename)
            for info in archive.infolist()
            if not info.is_dir()
        }
    script_name = next(
        (name for name in items if name.endswith(".data/scripts/agent-worker")),
        None,
    )
    if script_name is None:
        raise SystemExit(f"{wheel} has no agent-worker script")
    binary_name = script_name[: -len("agent-worker")] + "agent-worker.bin"
    payload = items[script_name]
    if payload.startswith(b"\x7fELF"):
        items[binary_name] = payload
    elif binary_name not in items:
        raise SystemExit(f"{wheel} agent-worker is not an ELF and {binary_name} is missing")
    items[script_name] = launcher
    record_name = next(name for name in list(items) if name.endswith(".dist-info/RECORD"))
    del items[record_name]
    rows = [_record_row(name, items[name]) for name in sorted(items)]
    rows.append(f"{record_name},,")
    items[record_name] = ("\n".join(rows) + "\n").encode()

    tmp = wheel.with_suffix(".whl.tmp")
    with zipfile.ZipFile(tmp, "w") as archive:
        for name, data in items.items():
            info = zipfile.ZipInfo(name)
            info.compress_type = zipfile.ZIP_DEFLATED
            mode = 0o100755 if name.endswith(("agent-worker", "agent-worker.bin")) else 0o100644
            info.external_attr = mode << 16
            archive.writestr(info, data)
    tmp.replace(wheel)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(f"usage: {sys.argv[0]} WHEEL LAUNCHER")
    rewrite_wheel(Path(sys.argv[1]), Path(sys.argv[2]).read_bytes())


if __name__ == "__main__":
    main()
