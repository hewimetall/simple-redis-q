# agent-worker

Rust supervisor and embedded CPython 3.12 runtime for Python task controllers.

The binary is `agent-worker`. The importable Python package is `simple_q`. The embedded Rust module is `simple_q_runtime`.

The decision for replacing Dramatiq, including the queue names `control` and `fanout`, is recorded in [docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md](docs/adr/0001-minimal-rust-runtime-instead-of-dramatiq.md).

`agent-worker admin` listens on `127.0.0.1:8080`. Set `BOARD_ADDR` only together with `BOARD_TOKEN` (at least 16 letters or digits). Open the board once as `/?token=...`; the token is then kept in an HttpOnly cookie. The compose file publishes that port on the host loopback and still requires `BOARD_TOKEN`, because the process inside the container listens on the container network.
