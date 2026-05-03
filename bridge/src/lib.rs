pub mod config;
pub mod connection;
pub mod c_api;
pub mod listener;
pub mod runtime;
pub mod task;

// Re-export c_api for integration tests
pub use c_api::*;

use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::RwLock;

// Simplified global state for TDD iterations
#[derive(Clone)]
pub struct BridgeState {
    pub next_task_id: Arc<std::sync::atomic::AtomicU64>,
    pub tasks: Arc<RwLock<std::collections::HashMap<u64, task::TaskResult>>>,
}

impl BridgeState {
    pub fn global() -> &'static Arc<Self> {
        static INSTANCE: OnceCell<Arc<BridgeState>> = OnceCell::new();
        INSTANCE.get_or_init(|| {
            Arc::new(BridgeState {
                next_task_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
                tasks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            })
        })
    }

    pub fn next_task_id(&self) -> u64 {
        self.next_task_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
