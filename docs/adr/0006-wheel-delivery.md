# ADR 0006 — Wheel delivery contract

- **Status:** Accepted
- **Date:** 2026-11-13

## Context

The brief asks for one installable Python wheel with an embedded Rust parent. Today `v1.2.0` ships **two** maturin artifacts: `simple-q-runtime` (extension) and `simple-q-worker` (bin, depends on runtime). Research text said “wheel family”, which blurred the requirement.

## Decision

1. **Product install:** `pip install simple-q-worker` (pulls `simple-q-runtime`) is the supported one-command install that yields the embedded parent binary + Python API.
2. **Artifacts:** keep the two-wheel maturin layout proven in CI (`simple-q-runtime` + `simple-q-worker`). This is the delivery mechanism, not a second product.
3. **Not in scope for the fork:** merging into a single `.whl` file on disk, or adding an external daemon.
4. E2E tests install from built wheels into an isolated venv and must not import the source tree.

## Consequences

Capability matrix and plan must say “one install command / two build artifacts”, not an open-ended family of distributions.
