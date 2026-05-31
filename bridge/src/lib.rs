pub mod c_api;
pub mod config;
pub mod connection;
pub mod listener;
pub mod logger;
pub mod runtime;
pub mod store;
pub mod task;

pub mod transport;

// Re-export c_api for integration tests
pub use c_api::*;

use once_cell::sync::OnceCell;

// Global config state
static CONFIG: OnceCell<Option<config::ReticulumConfig>> = OnceCell::new();

pub fn set_global_config(config: Option<config::ReticulumConfig>) {
    let _ = CONFIG.set(config);
}

pub fn get_global_config() -> Option<&'static config::ReticulumConfig> {
    CONFIG.get().and_then(|c| c.as_ref())
}
