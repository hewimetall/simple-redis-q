# ADR 0004 — Rust parent must not initialize CPython

- **Status:** Accepted (target contract); implementation pending Stage C
- **Date:** 2026-11-13

## Context

Hard constraint: one Python wheel with embedded Rust executable; Rust parent does not initialize CPython; CPython only in child. At `v1.2.0`, parent `worker` (non-`--child`) calls `runtime::initialize()` and `bridge::read_client` to load Redis URL and `Config` from the Python App before supervising.

## Decision

1. Parent composition uses `REDIS_URL` and `SIMPLE_Q_*` (and CLI flags) — the same sources enqueue/admin already trust for Redis.
2. Child remains the only process that initializes the interpreter and imports the App.
3. If product later requires App-derived settings unavailable via env, introduce an explicit ADR for a one-shot config export — not silent parent PyO3.
4. Add a process/unit guard that parent supervision path does not call interpreter init.

## Consequences

- `bridge::read_client` removed from parent startup path.
- Docs (`architecture.md`, `deploy.md`) updated when Stage C lands.
- Temporary dual path during migration is forbidden once the positive test exists.
