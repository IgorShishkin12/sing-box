use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connection::Connection;
use crate::listener::Listener;

#[derive(Debug, Clone)]
pub enum StoreEntry {
    Connection(Arc<Connection>),
    Listener(Arc<Listener>),
}

pub struct HandleStore {
    entries: RwLock<HashMap<u64, StoreEntry>>,
    next_handle: AtomicU64,
    /// Recycled handles returned by `remove`. Popped before allocating a fresh ID
    /// so the u64 space is never exhausted even under sustained churn.
    freed_handles: std::sync::Mutex<Vec<u64>>,
    /// Stores name → address-hash-hex mappings registered via `register_name`.
    name_to_hash: RwLock<HashMap<String, String>>,
}

impl HandleStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            freed_handles: std::sync::Mutex::new(Vec::new()),
            name_to_hash: RwLock::new(HashMap::new()),
        })
    }

    fn alloc_handle(&self) -> u64 {
        self.freed_handles
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| self.next_handle.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn insert_connection(&self, conn: Connection) -> u64 {
        let handle = self.alloc_handle();
        self.entries
            .write()
            .await
            .insert(handle, StoreEntry::Connection(Arc::new(conn)));
        handle
    }

    pub async fn insert_listener(&self, listener: Listener) -> u64 {
        let handle = self.alloc_handle();
        self.entries
            .write()
            .await
            .insert(handle, StoreEntry::Listener(Arc::new(listener)));
        handle
    }

    pub async fn get_connection(&self, handle: u64) -> Option<Arc<Connection>> {
        let entries = self.entries.read().await;
        match entries.get(&handle)? {
            StoreEntry::Connection(c) => Some(Arc::clone(c)),
            _ => None,
        }
    }

    pub async fn get_listener(&self, handle: u64) -> Option<Arc<Listener>> {
        let entries = self.entries.read().await;
        match entries.get(&handle)? {
            StoreEntry::Listener(l) => Some(Arc::clone(l)),
            _ => None,
        }
    }

    pub async fn remove(&self, handle: u64) -> Option<StoreEntry> {
        let removed = self.entries.write().await.remove(&handle);
        if removed.is_some() {
            self.freed_handles.lock().unwrap().push(handle);
        }
        removed
    }

    pub async fn clear_all(&self) {
        self.entries.write().await.clear();
        self.name_to_hash.write().await.clear();
        self.freed_handles.lock().unwrap().clear();
    }

    pub async fn register_name(&self, name: &str, hash: &str) {
        self.name_to_hash
            .write()
            .await
            .insert(name.to_string(), hash.to_string());
    }

    pub async fn get_hash_for_name(&self, name: &str) -> Option<String> {
        self.name_to_hash.read().await.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::listener::Listener;

    #[tokio::test]
    async fn test_handle_recycled_after_remove() {
        let store = HandleStore::new();
        let h1 = store.insert_connection(Connection::new()).await;
        let h2 = store.insert_connection(Connection::new()).await;
        assert_ne!(h1, h2);

        store.remove(h1).await;
        // Next alloc should reuse h1.
        let h3 = store.insert_connection(Connection::new()).await;
        assert_eq!(h3, h1, "freed handle should be recycled");
    }

    #[tokio::test]
    async fn test_handle_recycling_lifo() {
        let store = HandleStore::new();
        let h1 = store.insert_connection(Connection::new()).await;
        let h2 = store.insert_connection(Connection::new()).await;
        let h3 = store.insert_connection(Connection::new()).await;

        store.remove(h1).await;
        store.remove(h2).await;
        // LIFO: h2 was pushed last, popped first.
        let r1 = store.insert_connection(Connection::new()).await;
        let r2 = store.insert_connection(Connection::new()).await;
        assert_eq!(r1, h2);
        assert_eq!(r2, h1);
        // No more freed handles — allocates fresh.
        let r3 = store.insert_connection(Connection::new()).await;
        assert!(r3 > h3);
    }

    #[tokio::test]
    async fn test_clear_all_resets_freed_pool() {
        let store = HandleStore::new();
        let h = store.insert_connection(Connection::new()).await;
        store.remove(h).await;
        store.clear_all().await;
        // After clear the freed pool is empty; next alloc gets a fresh handle.
        let h2 = store.insert_connection(Connection::new()).await;
        assert!(store.get_connection(h2).await.is_some());
        store.clear_all().await;
    }

    #[tokio::test]
    async fn test_listener_handle_recycled() {
        let store = HandleStore::new();
        let lh = store.insert_listener(Listener::new()).await;
        store.remove(lh).await;
        let lh2 = store.insert_listener(Listener::new()).await;
        assert_eq!(lh2, lh, "listener handle should be recycled");
    }
}

static STORE: once_cell::sync::OnceCell<Arc<HandleStore>> = once_cell::sync::OnceCell::new();

pub fn global_store() -> &'static Arc<HandleStore> {
    STORE.get_or_init(HandleStore::new)
}
