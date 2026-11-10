# Publishing

Releases are cut from `main` by pushing a tag `vX.Y.Z`. The tag workflow builds two Linux distributions and publishes both to PyPI with Trusted Publishing when `PYPI_API_TOKEN` is empty.

| Distribution | Import or command | Build |
| --- | --- | --- |
| `simple-q-runtime` | `import simple_q` | maturin extension wheel, manylinux, CPython 3.12 |
| `simple-q-worker` | `agent-worker` | maturin binary wheel inside `python:3.12-slim-bookworm` |

The name `simple-q` is already used by another project. Do not publish under it.

## Version

Keep these three versions equal before tagging:

- `pyproject.toml` `version`
- `crates/simple-q-runtime/Cargo.toml` `version`
- `crates/agent-worker/Cargo.toml` `version`
- `crates/agent-worker/pyproject.toml` `version` and the pin `simple-q-runtime==` that same version

`agent-worker --version` prints the crate version. The worker package depends on that exact runtime version.

Neither crate is published to crates.io (`publish = false`).

## Wheels

`.github/workflows/release.yml` runs on tags `v*`. The smoke job installs `simple-q-runtime` and `simple-q-worker` at the tag version from PyPI. The simple index can lag the upload by a few seconds, so that install retries for about four minutes. For each of `x86_64` and `aarch64` the workflow:

1. Builds the `simple-q-runtime` wheel with maturin-action.
2. Builds `simple-q-worker` with `scripts/build-worker-wheel.sh`.

The script compiles inside `python:3.12-slim-bookworm` so the binary links the shared `libpython3.12.so.1.0` from that image. The manylinux image used for the extension does not ship that library. The worker build uses:

```bash
maturin build --release -b bin -i python3.12 --compatibility manylinux_2_36 --auditwheel skip
```

`manylinux_2_36` matches glibc 2.36 on Debian 12. Auditwheel is skipped because it would otherwise reject the link to libpython. The aarch64 job registers qemu binfmt before the container build.

The script then installs the already built runtime wheel and the new worker wheel into a fresh virtualenv and checks:

- `agent-worker --version` matches `importlib.metadata.version("simple-q-runtime")`
- `import simple_q` works with `PYTHONPATH` unset
- the wheel script's internal execute smoke check completes

## Trusted Publishing

The PyPI project is `simple-q-runtime`. A second project, `simple-q-worker`, needs its own pending publisher:

- owner `hewimetall`
- repository `simple-redis-q`
- workflow `release.yml`
- environment `pypi`

The workflow requests `id-token: write` and uses the environment `pypi`. If `PYPI_API_TOKEN` is set, the workflow uses that token instead. The GitHub Release uploads every file in `dist/`, including both wheels and the sdist of the runtime package.
