use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub enum TaskResult {
    Done { handle: u64, data: Vec<u8> },
    Error { message: String },
}

pub struct TaskRegistry {
    tasks: RwLock<HashMap<u64, TaskResult>>,
    next_id: AtomicU64,
}

impl TaskRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tasks: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        })
    }

    pub async fn insert(&self, result: TaskResult) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.tasks.write().await.insert(id, result);
        id
    }

    pub async fn get_and_remove(&self, id: u64) -> Option<TaskResult> {
        self.tasks.write().await.remove(&id)
    }
}

static REGISTRY: once_cell::sync::OnceCell<Arc<TaskRegistry>> = once_cell::sync::OnceCell::new();

pub fn global_registry() -> &'static Arc<TaskRegistry> {
    REGISTRY.get_or_init(TaskRegistry::new)
}
