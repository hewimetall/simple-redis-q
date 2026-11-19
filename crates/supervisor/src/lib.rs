//! Process lifecycle for one supervisor slot.
//!
//! Worker slots spawn in `agent-worker` through `watchexec-supervisor` 6.0.0.
//! This library does not depend on that crate. It keeps the slot lifecycle
//! policy. The metrics helper stays a direct `tokio::process` spawn.
//! `crates/supervisor/tests/process_compare.rs` checks SIGUSR1, the READY
//! handshake, exit status, process groups, and `pre_exec` descriptor
//! inheritance against `tokio::process`. The spawn hook exposes
//! `command_mut().pre_exec`, so the slot memfd and the parent pidfd still
//! clear `FD_CLOEXEC` in the child.

mod lifecycle;

pub use lifecycle::{ProcessEvent, ProcessPhase, ProcessTransitionError, SlotLifecycle, SlotOwner};
