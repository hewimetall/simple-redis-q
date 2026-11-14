# ADR 0005 — Source license is undeclared at v1.2.0

- **Status:** Accepted (risk record)
- **Date:** 2026-11-13

## Context

GitHub reports no license for `hewimetall/simple-redis-q`. The `v1.2.0` tree has no `LICENSE*` file. Included candidates (Apalis, Oxana, watchexec-supervisor, Tokio, PyO3, redis-rs) are MIT and/or Apache-2.0.

## Decision

1. Record the gap as a release blocker for public redistribution of the fork until maintainers add an SPDX license.
2. Do not invent a license file in this research PR without explicit owner approval.
3. Dependency licenses are compatible with a future MIT OR Apache-2.0 choice; final choice is the owner's.

## Consequences

- Research and internal architectural work may continue on a branch.
- The tree on main still publishes `simple-q-runtime` and `simple-q-worker` from a version tag. This record does not change that workflow. The research example is not a workspace member and is not published.
- A public redistribution of the fork stays blocked until the repository has an SPDX license. This record does not add one.
