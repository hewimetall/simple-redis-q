//! Process lifecycle for one supervisor slot.
//!
//! Production spawning stays in `agent-worker` on `tokio::process`.
//! `watchexec-supervisor` is measured from the integration test and is not a
//! dependency of this library.

mod lifecycle;

pub use lifecycle::{ProcessEvent, ProcessPhase, ProcessTransitionError, SlotLifecycle, SlotOwner};

/// Worker slots spawn through `watchexec-supervisor` 6.0.0.
///
/// `crates/supervisor/tests/process_compare.rs` checks SIGUSR1, the READY
/// handshake, exit status, process groups, and `pre_exec` descriptor
/// inheritance against `tokio::process`. The spawn hook exposes
/// `command_mut().pre_exec`, so the slot memfd and the parent pidfd still
/// clear `FD_CLOEXEC` in the child. The metrics helper stays a direct spawn.
pub const PROCESS_PORT_IS_WATCHEXEC: bool = true;
