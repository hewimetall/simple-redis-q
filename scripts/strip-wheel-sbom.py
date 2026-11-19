#!/usr/bin/env python3
"""Drop generated SBOMs from wheels and rewrite RECORD.

Maturin writes a CycloneDX file that copies dependency homepages. Those
pages are not configuration. The wheel stays installable.
"""

import base64
import hashlib
import pathlib
import sys
import zipfile


def strip(wheel: pathlib.Path) -> None:
    tmp = wheel.with_suffix(wheel.suffix + ".tmp")
    with zipfile.ZipFile(wheel) as src, zipfile.ZipFile(tmp, "w") as dst:
        record_info = None
        kept = []
        for info in src.infolist():
            if ".dist-info/sboms/" in info.filename:
                continue
            data = src.read(info.filename)
            if info.filename.endswith(".dist-info/RECORD"):
                record_info = info
                continue
            kept.append((info, data))
        if record_info is None:
            raise SystemExit(f"{wheel} has no RECORD")
        lines = []
        for info, data in kept:
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
            lines.append(f"{info.filename},sha256={digest},{len(data)}")
            dst.writestr(info, data)
        lines.append(f"{record_info.filename},,")
        dst.writestr(record_info, ("\n".join(lines) + "\n").encode())
    tmp.replace(wheel)


def main() -> None:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    wheels = [root] if root.is_file() else sorted(root.glob("*.whl"))
    if not wheels:
        raise SystemExit(f"no wheels in {root}")
    for wheel in wheels:
        strip(wheel)
        print(wheel.name)


if __name__ == "__main__":
    main()
