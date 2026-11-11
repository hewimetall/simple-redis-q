#!/bin/bash
# Build simple-q-worker inside manylinux_2_28.
#
# The image has glibc 2.28 and CPython 3.12's libpython3.12.so.1.0. A binary
# linked there runs on Ubuntu 22.04 and RHEL 9. The Debian 12 image is glibc
# 2.36, which is too new for those hosts. auditwheel stays skipped because the
# ELF keeps its NEEDED entry for libpython instead of vendoring it.
set -euo pipefail

platform="${1:-linux/amd64}"
root="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$root/dist"

case "$platform" in
  linux/amd64) image=quay.io/pypa/manylinux_2_28_x86_64 ;;
  linux/arm64) image=quay.io/pypa/manylinux_2_28_aarch64 ;;
  *)
    echo "unsupported platform: $platform" >&2
    exit 1
    ;;
esac

docker run --rm --platform "$platform" -i \
  -v "$root":/io \
  -w /io \
  "$image" \
  bash -s <<'EOF'
set -euo pipefail
# The image's /opt/python builds are not --enable-shared, so there is no
# libpython3.12.so to link. Build one here. The container's glibc is 2.28,
# so this library does not raise the wheel's glibc requirement.
yum install -y gcc make openssl-devel zlib-devel libffi-devel bzip2-devel xz-devel readline-devel sqlite-devel pkgconf-pkg-config
python_version=3.12.16
curl -fsSL "https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz" -o /tmp/python.tgz
tar -C /tmp -xzf /tmp/python.tgz
(
  cd "/tmp/Python-${python_version}"
  ./configure --prefix=/opt/python-shared --enable-shared --with-ensurepip=install
  make -j"$(nproc)"
  make install
)
echo /opt/python-shared/lib > /etc/ld.so.conf.d/python-shared.conf
ldconfig
test -f /opt/python-shared/lib/libpython3.12.so.1.0
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"
export PATH="/opt/python-shared/bin:${PATH}"
export LD_LIBRARY_PATH=/opt/python-shared/lib
export PYO3_PYTHON=/opt/python-shared/bin/python3.12
# The host job compiles into /io/target on Ubuntu 24.04. Those build scripts
# need glibc 2.34, and this image has 2.28. Cargo would run the host binaries
# and abort before it compiles anything here.
export CARGO_TARGET_DIR=/tmp/cargo-target
python3.12 -m pip install --upgrade pip
python3.12 -m pip install "maturin>=1.16,<2" uv
cd crates/agent-worker
maturin build --release -b bin -i python3.12 --compatibility manylinux_2_28 --auditwheel skip --out /tmp/wheels
cd /io
python3.12 /io/scripts/install_worker_launcher.py /tmp/wheels/simple_q_worker-*.whl /io/scripts/agent-worker-launch.sh
python3.12 - << 'PY'
import pathlib, re, subprocess, sys, zipfile
wheel = next(pathlib.Path("/tmp/wheels").glob("simple_q_worker-*.whl"))
with zipfile.ZipFile(wheel) as archive:
    elf = next(name for name in archive.namelist() if name.endswith("scripts/agent-worker.bin"))
    archive.extract(elf, "/tmp/elf")
path = pathlib.Path("/tmp/elf") / elf
text = subprocess.check_output(["objdump", "-T", str(path)], text=True, errors="replace")
versions = []
for match in re.findall(r"GLIBC_([0-9]+(?:\.[0-9]+)*)", text):
    versions.append(tuple(int(part) for part in match.split(".")))
if not versions:
    sys.exit("agent-worker.bin has no GLIBC symbols")
highest = max(versions)
if highest > (2, 28):
    sys.exit(f"agent-worker.bin needs GLIBC_{'.'.join(map(str, highest))}, want <= 2.28")
print("glibc", ".".join(map(str, highest)))
PY
ver=$(basename /tmp/wheels/simple_q_worker-*.whl | cut -d- -f2)
if ! compgen -G "/io/dist/simple_q_runtime-${ver}-"*.whl > /dev/null; then
  (cd /io && maturin build --release --out dist -i python3.12 --compatibility manylinux_2_28)
fi
uv venv --python /opt/python-shared/bin/python3.12 /opt/venv
uv pip install --python /opt/venv/bin/python \
  /io/dist/simple_q_runtime-${ver}-*.whl \
  /tmp/wheels/simple_q_worker-${ver}-*.whl
bin_ver="$(/opt/venv/bin/agent-worker --version | awk '{print $NF}')"
py_ver="$(/opt/venv/bin/python -c 'import importlib.metadata as m; print(m.version("simple-q-runtime"))')"
python3.12 - "$bin_ver" "$py_ver" << 'PY'
import re
import sys

def pep440(version):
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

left, right = sys.argv[1:]
if pep440(left) != pep440(right):
    raise SystemExit(f"agent-worker {left} != simple-q-runtime {right}")
PY
env -u PYTHONPATH /opt/venv/bin/python -c 'import simple_q'
mkdir -p /tmp/smokeapp
cat > /tmp/smokeapp/smoke_app.py << 'PY'
from simple_q import App, MemoryClient, actor


@actor(queue="default")
async def demo(task_id: str) -> str:
    return task_id


def create_app() -> App:
    return App(client=MemoryClient(), actors=[demo])
PY
env PYTHONPATH=/tmp/smokeapp /opt/venv/bin/agent-worker execute --factory --app smoke_app:create_app --task demo
cp /tmp/wheels/simple_q_worker-*.whl /io/dist/
EOF
