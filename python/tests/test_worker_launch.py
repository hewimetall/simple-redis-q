"""The wheel launcher finds libpython next to the base interpreter."""

import os
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / "scripts" / "agent-worker-launch.sh"
sys.path.insert(0, str(ROOT / "scripts"))

from install_worker_launcher import rewrite_wheel


def test_launcher_points_ld_library_path_at_the_base_lib(tmp_path):
    base = tmp_path / "cpython" / "lib"
    base.mkdir(parents=True)
    (base / "libpython3.12.so.1.0").write_bytes(b"")
    (tmp_path / "cpython" / "bin").mkdir()
    venv = tmp_path / "venv"
    bindir = venv / "bin"
    bindir.mkdir(parents=True)
    (venv / "pyvenv.cfg").write_text(
        f"home = {tmp_path / 'cpython' / 'bin'}\nversion = 3.12.16\n"
    )
    elf = bindir / "agent-worker.bin"
    elf.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"${LD_LIBRARY_PATH-}\"\nprintf '%s\\n' \"$1\"\n"
    )
    elf.chmod(elf.stat().st_mode | stat.S_IEXEC)
    script = bindir / "agent-worker"
    script.write_bytes(LAUNCHER.read_bytes())
    script.chmod(script.stat().st_mode | stat.S_IEXEC)

    env = os.environ.copy()
    env.pop("LD_LIBRARY_PATH", None)
    completed = subprocess.run(
        [str(script), "info"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    lines = completed.stdout.splitlines()
    assert lines == [str(base), "info"]


def test_launcher_uses_the_version_minor_not_another_libpython(tmp_path):
    tree = tmp_path / "cpython"
    (tree / "lib").mkdir(parents=True)
    (tree / "lib64").mkdir()
    (tree / "bin").mkdir()
    (tree / "lib" / "libpython3.12.so.1.0").write_bytes(b"")
    (tree / "lib64" / "libpython3.15.so.1.0").write_bytes(b"")
    venv = tmp_path / "venv"
    bindir = venv / "bin"
    bindir.mkdir(parents=True)
    (venv / "pyvenv.cfg").write_text(
        f"home = {tree / 'bin'}\nversion = 3.15.2\n"
    )
    elf = bindir / "agent-worker.bin"
    elf.write_text("#!/bin/sh\nprintf '%s\\n' \"${LD_LIBRARY_PATH-}\"\n")
    elf.chmod(elf.stat().st_mode | stat.S_IEXEC)
    script = bindir / "agent-worker"
    script.write_bytes(LAUNCHER.read_bytes())
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    env = os.environ.copy()
    env.pop("LD_LIBRARY_PATH", None)
    completed = subprocess.run([str(script)], check=True, capture_output=True, text=True, env=env)
    assert completed.stdout.strip() == str(tree / "lib64")


def test_launcher_leaves_the_path_alone_without_libpython(tmp_path):
    venv = tmp_path / "venv"
    bindir = venv / "bin"
    bindir.mkdir(parents=True)
    (venv / "pyvenv.cfg").write_text(f"home = {tmp_path / 'missing' / 'bin'}\n")
    elf = bindir / "agent-worker.bin"
    elf.write_text("#!/bin/sh\nprintf '%s\\n' \"${LD_LIBRARY_PATH-unset}\"\n")
    elf.chmod(elf.stat().st_mode | stat.S_IEXEC)
    script = bindir / "agent-worker"
    script.write_bytes(LAUNCHER.read_bytes())
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    env = os.environ.copy()
    env.pop("LD_LIBRARY_PATH", None)
    completed = subprocess.run([str(script)], check=True, capture_output=True, text=True, env=env)
    assert completed.stdout.strip() == "unset"


def test_rewrite_wheel_keeps_the_elf_behind_the_launcher(tmp_path):
    wheel = tmp_path / "simple_q_worker-1.0.0rc2-py3-none-any.whl"
    script = "simple_q_worker-1.0.0rc2.data/scripts/agent-worker"
    record = "simple_q_worker-1.0.0rc2.dist-info/RECORD"
    elf = b"\x7fELF-fake"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(script, elf)
        archive.writestr(record, f"{script},,\n{record},,\n")
    rewrite_wheel(wheel, LAUNCHER.read_bytes())
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        assert script in names
        assert script + ".bin" in names
        assert archive.read(script + ".bin") == elf
        assert archive.read(script).startswith(b"#!/bin/sh\n")
        record_text = archive.read(record).decode()
    assert script in record_text
    assert script + ".bin" in record_text
    assert record_text.strip().splitlines()[-1] == f"{record},,"
