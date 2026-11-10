//! Library shared by the worker binary and the importable Python module.
//!
//! Business types are `job`, `names`, `config`, and `ping`.
//! `ids` reads `HOSTNAME` or `/etc/hostname` for the worker name.
//! Server side is `redis_api`, `lock`, `cleanup`, and `slots`.
//! User side is `python` (the importable module) and `registry` (in-process executions).

// Business logic.
pub mod config;
pub mod ids;
pub mod job;
pub mod names;
pub mod ping;

// Server side. Redis, locks, and metric files.
pub mod admin;
pub mod cleanup;
pub mod lock;
pub mod redis_api;
pub mod slots;

// User side. The importable module and the in-process execution table.
pub mod python;
pub mod registry;

pub use job::Job;
pub use names::MESSAGE_VERSION;
