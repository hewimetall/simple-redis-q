# ADR 0004 — Parent without CPython; config compatibility

- **Status:** Accepted (binding for Stage C)
- **Date:** 2026-11-13
- **Code sketch:** `crates/simple-q-core::parent`

## Context

`v1.2.0` parent `worker` (non-`--child`) calls `runtime::initialize()` and `bridge::read_client` to load Redis URL and `Config` from the Python `App`. Target contract forbids CPython in the parent. Simply deleting that path changes behavior for apps that only set Redis on `App.client`.

## Decision

### Config sources

| Process | Source of truth | Priority |
| --- | --- | --- |
| Parent (supervise) | CLI flags + `REDIS_URL` + `SIMPLE_Q_*` + defaults | CLI > env > defaults |
| Child (queue workers) | Unchanged: import `App`, use `App.client` for Redis URL / settings used while executing | App object as today |
| Producer (Python) | `Client` / `App` as today | unchanged |

Parent **must not** call `bridge::read_client` or `runtime::initialize`.

### Compatibility

1. Document that operators must set `REDIS_URL` (and relevant `SIMPLE_Q_*`) for the supervisor process — same variables `enqueue` / `admin` already use.
2. Child continues to read programmatic `App` settings (timeouts, hooks, client URL).
3. **Mismatch detection:** child logs effective Redis URL (host/db, no password) at start; parent stores the same fields on the supervisor Redis record; `agent-worker info` / docs warn when they differ.
4. One-shot “config export child” is **not** required for Stage C. Revisit only if product proves env cannot express a required parent setting.

### Code gate

`ParentBootstrap::try_from_legacy_app_client()` returns `AppClientRequiresCPython`.  
`PARENT_NO_CPYTHON_IMPLEMENTED` flips to `true` only when production parent path is fixed.

## Consequences

- Existing deployments that relied on parent reading `App.client` alone need `REDIS_URL` in the supervisor environment (document in migration notes when Stage C lands).
- Child programmatic config remains valid; no requirement to move all settings to env.
