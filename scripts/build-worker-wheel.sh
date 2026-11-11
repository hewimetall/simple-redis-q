#!/bin/bash
# Build simple-q-worker inside manylinux_2_28, once per CPython minor.
#
# The image has glibc 2.28. Its /opt/python builds are not --enable-shared, so
# there is no libpython3.X.so to link. This script builds a shared CPython for
# each minor (GIL only) and links one ELF to that soname. auditwheel stays
# skipped because the ELF keeps its NEEDED entry for libpython. A second
# argument overrides the version list, for example "3.12.16".
set -euo pipefail

platform="${1:-linux/amd64}"
versions="${2:-3.12.16 3.13.18 3.14.10 3.15.2}"
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
  -e PYTHON_VERSIONS="$versions" \
  -v "$root":/io \
  -w /io \
  "$image" \
  bash -s <<'EOF'
set -euo pipefail
yum install -y gcc make openssl-devel zlib-devel libffi-devel bzip2-devel xz-devel readline-devel sqlite-devel pkgconf-pkg-config
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"
# rust-toolchain.toml also requests rustfmt and clippy. On aarch64 that
# install collides with the cargo-fmt proxy and the wheel build stops.
# The wheel only needs the compiler named in the toolchain file.
channel=$(sed -n 's/^channel = "\(.*\)"/\1/p' /io/rust-toolchain.toml)
rustup toolchain install "$channel" --profile minimal --no-self-update
export RUSTUP_TOOLCHAIN="$channel"
# The host job compiles into /io/target on Ubuntu 24.04. Those build scripts
# need glibc 2.34, and this image has 2.28. Cargo would run the host binaries
# and abort before it compiles anything here.
export CARGO_TARGET_DIR=/tmp/cargo-target
tool=/opt/python/cp312-cp312/bin/python
"$tool" -m pip install --upgrade pip
"$tool" -m pip install "maturin>=1.16,<2" uv
export PATH="/opt/python/cp312-cp312/bin:${PATH}"

mkdir -p /tmp/smokeapp /tmp/wheels
cat > /tmp/smokeapp/smoke_app.py << 'PY'
from simple_q import App, MemoryClient, actor


@actor(queue="default")
async def demo(task_id: str) -> str:
    return task_id


def create_app() -> App:
    return App(client=MemoryClient(), actors=[demo])
PY

build_shared() {
  local ver="$1"
  local mm="${ver%.*}"
  local prefix="/opt/python-shared/${ver}"
  curl -fsSL "https://www.python.org/ftp/python/${ver}/Python-${ver}.tgz" -o "/tmp/python-${ver}.tgz"
  rm -rf "/tmp/Python-${ver}"
  tar -C /tmp -xzf "/tmp/python-${ver}.tgz"
  (
    cd "/tmp/Python-${ver}"
    ./configure --prefix="$prefix" --enable-shared --with-ensurepip=install
    make -j"$(nproc)"
    make install
  )
  echo "${prefix}/lib" > "/etc/ld.so.conf.d/python-shared-${mm}.conf"
  ldconfig
  test -f "${prefix}/lib/libpython${mm}.so.1.0"
}

for ver in $PYTHON_VERSIONS; do
  mm="${ver%.*}"
  tag="cp${mm/./}"
  prefix="/opt/python-shared/${ver}"
  build_shared "$ver"
  export LD_LIBRARY_PATH="${prefix}/lib"
  export PYO3_PYTHON="${prefix}/bin/python${mm}"
  (
    cd /io/crates/agent-worker
    maturin build --release -b bin -i "$PYO3_PYTHON" --compatibility manylinux_2_28 --auditwheel skip --out /tmp/wheels
  )
  wheel=$(ls -1 /tmp/wheels/simple_q_worker-*-"${tag}"-"${tag}"-*.whl | tail -n 1)
  "$tool" /io/scripts/install_worker_launcher.py "$wheel" /io/scripts/agent-worker-launch.sh
  "$tool" - "$wheel" << 'PY'
import pathlib, re, subprocess, sys, zipfile
wheel = pathlib.Path(sys.argv[1])
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
print(wheel.name, "glibc", ".".join(map(str, highest)))
PY
  pkg=$(basename "$wheel" | cut -d- -f2)
  if ! compgen -G "/io/dist/simple_q_runtime-${pkg}-${tag}-${tag}-"*.whl > /dev/null; then
    (cd /io && maturin build --release --out dist -i "$PYO3_PYTHON" --compatibility manylinux_2_28)
  fi
  runtime=$(ls -1 /io/dist/simple_q_runtime-"${pkg}"-"${tag}"-"${tag}"-*.whl | tail -n 1)
  venv="/opt/venv-${tag}"
  uv venv --python "$PYO3_PYTHON" "$venv"
  uv pip install --python "${venv}/bin/python" "$runtime" "$wheel"
  bin_ver="$(env -u LD_LIBRARY_PATH "${venv}/bin/agent-worker" --version | awk '{print $NF}')"
  py_ver="$("${venv}/bin/python" -c 'import importlib.metadata as m; print(m.version("simple-q-runtime"))')"
  "$tool" - "$bin_ver" "$py_ver" << 'PY'
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
  env -u PYTHONPATH -u LD_LIBRARY_PATH "${venv}/bin/python" -c 'import simple_q'
  env -u LD_LIBRARY_PATH PYTHONPATH=/tmp/smokeapp "${venv}/bin/agent-worker" execute --factory --app smoke_app:create_app --task demo
  cp "$wheel" /io/dist/
done
EOF
