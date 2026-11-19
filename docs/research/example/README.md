# Research example

This crate is an example of the fork contracts. It is not a member of the simple-q workspace. `agent-worker` and `simple-q-runtime` do not depend on it. `cargo test --workspace` and the CI Rust job do not build it.

The types are `AttemptLifecycle`, `AdmissionGate`, `ParentBootstrap`, and the port enums. The workspace copies live in `crates/runtime`, `crates/backend`, `crates/supervisor`, `crates/python-bridge`, and `crates/cli`. `PARENT_NO_CPYTHON_IMPLEMENTED` in `simple-q-parent` is `true`, and the workspace gate test is enabled. This example crate is still the stage-B sketch: its constant is `false`, and its gate test stays ignored.

```bash
cargo test --manifest-path docs/research/example/simple-q-core/Cargo.toml
```

One test is ignored. It stays ignored until a later stage changes the parent. That stage is not this tree.
