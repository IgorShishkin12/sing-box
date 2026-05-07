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

    /// Insert a pending task (no result yet). Returns task ID.
    pub async fn insert_pending(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        // We don't insert anything for pending - get_and_remove will return None
        id
    }

    /// Complete a pending task with a result.
    pub async fn complete(&self, id: u64, result: TaskResult) -> bool {
        self.tasks.write().await.insert(id, result).is_none()
    }

    /// Clear all tasks from the registry.
    pub async fn clear_all(&self) {
        self.tasks.write().await.clear();
    }
}

static REGISTRY: once_cell::sync::OnceCell<Arc<TaskRegistry>> = once_cell::sync::OnceCell::new();

pub fn global_registry() -> &'static Arc<TaskRegistry> {
    REGISTRY.get_or_init(TaskRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_task_registry_insert_and_get() {
        let registry = TaskRegistry::new();
        let result = TaskResult::Done { handle: 42, data: vec![1, 2, 3] };
        let id = registry.insert(result.clone()).await;
        let retrieved = registry.get_and_remove(id).await;
        assert!(matches!(retrieved, Some(TaskResult::Done { handle: 42, data: _ })));
        // Should be removed
        let again = registry.get_and_remove(id).await;
        assert!(again.is_none());
    }

    #[tokio::test]
    async fn test_task_registry_pending_and_complete() {
        let registry = TaskRegistry::new();
        let id = registry.insert_pending().await;
        // Pending task should not be retrievable yet
        let retrieved = registry.get_and_remove(id).await;
        assert!(retrieved.is_none());
        // Complete it
        let completed = registry.complete(id, TaskResult::Done { handle: 99, data: vec![] }).await;
        assert!(completed);
        // Now should be retrievable
        let retrieved = registry.get_and_remove(id).await;
        assert!(matches!(retrieved, Some(TaskResult::Done { handle: 99, data: _ })));
    }

    #[tokio::test]
    async fn test_task_registry_error_result() {
        let registry = TaskRegistry::new();
        let result = TaskResult::Error { message: "test error".to_string() };
        let id = registry.insert(result).await;
        let retrieved = registry.get_and_remove(id).await;
        assert!(matches!(retrieved, Some(TaskResult::Error { message: _ })));
    }
}
