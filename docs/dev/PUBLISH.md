# Publishing

Releases are cut from `main` by pushing a tag `vX.Y.Z`. The tag workflow builds two Linux distributions and publishes both to PyPI with Trusted Publishing when `PYPI_API_TOKEN` is empty.

| Distribution | Import or command | Build |
| --- | --- | --- |
| `simple-q-runtime` | `import simple_q` | maturin extension wheel, manylinux, CPython 3.12 |
| `simple-q-worker` | `agent-worker` | maturin binary wheel inside `manylinux_2_28`, launcher in front of the ELF |

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

`.github/workflows/release.yml` runs on tags `v*`. For each of `x86_64` and `aarch64` it:

1. Builds the `simple-q-runtime` wheel with maturin-action.
2. Builds `simple-q-worker` with `scripts/build-worker-wheel.sh`.

The script compiles inside `quay.io/pypa/manylinux_2_28`. That image's CPython is not a shared build, so the script first builds CPython 3.12 with `--enable-shared`. Debian 12 is glibc 2.36, so a binary built there does not run on Ubuntu 22.04 or RHEL 9. The worker build uses:

```bash
maturin build --release -b bin -i python3.12 --compatibility manylinux_2_28 --auditwheel skip
```

Auditwheel is skipped because it would otherwise reject the link to libpython. The build then fails if `agent-worker.bin` needs a glibc newer than 2.28. It installs a `#!/bin/sh` launcher as `agent-worker` and keeps the ELF as `agent-worker.bin`. The launcher reads `pyvenv.cfg` `home` and prepends that interpreter's `lib` to `LD_LIBRARY_PATH`. The ELF's RPATH is `$ORIGIN/../lib`. The aarch64 job registers qemu binfmt before the container build.

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
