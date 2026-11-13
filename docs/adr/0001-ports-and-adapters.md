# ADR 0001 — Ports & Adapters as the architectural foundation

- **Status:** Accepted (Stage B)
- **Date:** 2026-11-13
- **Baseline:** simple-redis-q `v1.2.0` (`9164eeab0361639d8cd2d0ca2bb91c355710c1a8`)

## Context

At `v1.2.0` queue Redis types, PyO3, process supervision, and delivery policy are intertwined across `agent-worker` and `simple-q-runtime`. The fork requires testable boundaries, replaceable process/queue adapters, and a parent that does not initialize CPython.

## Decision

Adopt Ports & Adapters ([Cockburn hexagonal architecture](https://alistair.cockburn.us/hexagonal-architecture)):

- Core defines ports from **needed** interactions (queue attempt lifecycle, process control, Python bridge outcomes, clock/config).
- Adapters implement Redis/Apalis (or future backend), OS processes, and PyO3.
- Composition root lives in `crates/cli`.
- No trait-per-type; no universal multi-backend framework.

## Consequences

- Mass code moves wait until ports and state machines are specified (this ADR + `docs/architecture/plan.md`).
- New capabilities follow Agent TDD against the relevant port level.
- Apalis/Oxana/watchexec choices become adapter decisions, not core rewrites.
