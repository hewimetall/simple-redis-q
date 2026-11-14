# Research example

This crate is an example of the fork contracts. It is not a member of the simple-q workspace. `agent-worker` and `simple-q-runtime` do not depend on it. `cargo test --workspace` and the CI Rust job do not build it.

The types are `AttemptLifecycle`, `AdmissionGate`, `ParentBootstrap`, and the port enums. `PARENT_NO_CPYTHON_IMPLEMENTED` is `false`. The parent that ships still loads the `App`.

```bash
cargo test --manifest-path docs/research/example/simple-q-core/Cargo.toml
```

One test is ignored. It stays ignored until a later stage changes the parent. That stage is not this tree.
