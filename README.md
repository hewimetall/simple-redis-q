# agent-worker

Rust supervisor and embedded CPython 3.12 runtime for Python task controllers.

The binary is `agent-worker`. The importable Python package is `simple_q`. The embedded Rust module is `simple_q_runtime`.

The decision for replacing Dramatiq, including the queue names `control` and `fanout`, is recorded in [docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md](docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md).
