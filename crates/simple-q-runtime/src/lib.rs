//! Library shared by the worker binary and the importable Python module.
//!
//! Importing the Python module does not start Tokio, spawn threads, or open Redis.
//! `install` does that, and only when a producer calls `configure`.

pub mod cleanup;
pub mod ids;
pub mod job;
pub mod lock;
pub mod names;
pub mod ping;
pub mod python;
pub mod redis_api;
pub mod registry;
pub mod slots;

pub use job::Job;
pub use names::MESSAGE_VERSION;
