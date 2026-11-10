//! Library shared by the worker binary and the importable Python module.
//!
//! Importing the Python module does not start Tokio, spawn threads, or open Redis.
//! `install` does that, and only when a producer calls `configure`.
//!
//! One crate, three sides. Dependencies point at the business types.
//! A further crate would only rename these modules; the binary is already the other adapter.
//!
//! Business logic does not open Redis and does not talk to Python: `job`, `names`.
//! `ids` is the same layer except `hostname`, which reads the environment.
//! `ping` is the silence rule for the interpreter. It has no Redis either.
//!
//! Server side, driven by that logic: `redis_api`, `lock`, `cleanup`, `slots`.
//!
//! User side, which drives the library: `python` (the importable module) and
//! `registry` (executions of this process). The worker binary is the other
//! user-side adapter.

// Business logic.
pub mod ids;
pub mod job;
pub mod names;
pub mod ping;

// Server side. Redis, locks, and metric files.
pub mod cleanup;
pub mod lock;
pub mod redis_api;
pub mod slots;

// User side. The importable module and the in-process execution table.
pub mod python;
pub mod registry;

pub use job::Job;
pub use names::MESSAGE_VERSION;
