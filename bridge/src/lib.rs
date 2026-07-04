pub mod c_api;
pub mod config;
pub mod connection;
pub mod listener;
pub mod logger;
pub mod runtime;
pub mod store;

pub mod transport;

// Re-export c_api for integration tests
pub use c_api::*;

/// Inactivity timeout (seconds) for a Resource transfer in either direction.
/// Reset on every progress event, so it never caps total transfer duration —
/// only genuine silence (peer stopped acking) for this long aborts the transfer.
/// Shared by the outbound (connection.rs) and inbound (transport.rs) paths so the
/// two cannot drift apart.
pub(crate) const RESOURCE_INACTIVITY_SECS: u64 = 180;

use once_cell::sync::OnceCell;

// Global config state
static CONFIG: OnceCell<Option<config::ReticulumConfig>> = OnceCell::new();

pub fn set_global_config(config: Option<config::ReticulumConfig>) {
    let _ = CONFIG.set(config);
}

pub fn get_global_config() -> Option<&'static config::ReticulumConfig> {
    CONFIG.get().and_then(|c| c.as_ref())
}
