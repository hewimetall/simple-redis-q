#!/bin/bash
# Build simple-q-worker inside python:3.12-slim-bookworm.
#
# The manylinux image used for the extension wheel ships CPython without
# libpython3.12.so. The supervisor binary links that shared library and must
# be built where it exists. manylinux_2_36 matches the glibc in Debian 12.
set -euo pipefail

platform="${1:-linux/amd64}"
root="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$root/dist"

docker run --rm --platform "$platform" -i \
  -v "$root":/io \
  -w /io \
  python:3.12-slim-bookworm \
  bash -s <<'EOF'
set -euo pipefail
apt-get update
apt-get install -y --no-install-recommends build-essential ca-certificates curl pkg-config libssl-dev
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"
python -m pip install --upgrade pip
python -m pip install "maturin>=1.8,<2" uv
cd crates/agent-worker
maturin build --release -b bin -i python3.12 --compatibility manylinux_2_36 --auditwheel skip --out /tmp/wheels
cd /io
uv venv /opt/venv
uv pip install --python /opt/venv/bin/python dist/simple_q_runtime-*.whl /tmp/wheels/simple_q_worker-*.whl
bin_ver="$(/opt/venv/bin/agent-worker --version | awk '{print $NF}')"
py_ver="$(/opt/venv/bin/python -c 'import importlib.metadata as m; print(m.version("simple-q-runtime"))')"
test "$bin_ver" = "$py_ver"
env -u PYTHONPATH /opt/venv/bin/python -c 'import simple_q'
env -u PYTHONPATH /opt/venv/bin/agent-worker execute --app simple_q.app:app --task demo
cp /tmp/wheels/simple_q_worker-*.whl /io/dist/
EOF
