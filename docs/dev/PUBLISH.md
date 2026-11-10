# Publishing simple-q-runtime

## One-time PyPI setup

Create the PyPI project `simple-q-runtime` and configure Trusted Publishing:

- Owner: the PyPI account or organization that owns the package.
- Publisher: GitHub.
- Repository owner: `hewimetall`.
- Repository name: `simple-redis-q`.
- Workflow name: `release.yml`.
- Environment name: `pypi`.

The release workflow publishes from the GitHub environment `pypi` and uses the
project URL `https://pypi.org/p/simple-q-runtime`.

The distribution name is `simple-q-runtime` because `simple-q` is already
registered on PyPI by another project. The import package remains `simple_q`.
The `agent-worker` binary is not part of this wheel.

If Trusted Publishing is not configured yet, add a repository or environment
secret named `PYPI_API_TOKEN`. The workflow uses Trusted Publishing when this
secret is absent and uses the token fallback when the secret is present.

## Release steps

1. Bump the version in `pyproject.toml`.
2. Keep the Rust package version in `crates/simple-q-runtime/Cargo.toml` in sync
   with the Python version.
3. Run validation:

   ```bash
   cargo test --workspace
   python3 -m pytest python/tests -q
   ```

4. Commit the version change.
5. Create and push a `v*` tag, for example:

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

6. GitHub Actions builds CPython 3.12 wheels, an sdist, creates a GitHub
   Release, and publishes to PyPI.

## Install the published package

```bash
pip install simple-q-runtime
```
