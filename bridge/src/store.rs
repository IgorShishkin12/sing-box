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
    listener_hashes: RwLock<HashMap<String, u64>>,
    /// Stores name → address-hash-hex mappings registered via `register_name`.
    name_to_hash: RwLock<HashMap<String, String>>,
}

impl HandleStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            listener_hashes: RwLock::new(HashMap::new()),
            name_to_hash: RwLock::new(HashMap::new()),
        })
    }

    pub async fn insert_connection(&self, conn: Connection) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.entries
            .write()
            .await
            .insert(handle, StoreEntry::Connection(Arc::new(conn)));
        handle
    }

    pub async fn insert_listener(&self, listener: Listener) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        let hash = listener.hash().map(|h| h.to_string());
        self.entries
            .write()
            .await
            .insert(handle, StoreEntry::Listener(Arc::new(listener)));
        if let Some(h) = hash {
            self.listener_hashes.write().await.insert(h, handle);
        }
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

    pub async fn get_listener_by_hash(&self, hash: &str) -> Option<Arc<Listener>> {
        let hashes = self.listener_hashes.read().await;
        let handle = hashes.get(hash)?;
        let entries = self.entries.read().await;
        match entries.get(handle)? {
            StoreEntry::Listener(l) => Some(Arc::clone(l)),
            _ => None,
        }
    }

    pub async fn remove(&self, handle: u64) -> Option<StoreEntry> {
        let entry = self.entries.write().await.remove(&handle);
        if let Some(StoreEntry::Listener(ref l)) = entry {
            if let Some(h) = l.hash() {
                self.listener_hashes.write().await.remove(h);
            }
        }
        entry
    }

    /// Clear all entries from the store.
    pub async fn clear_all(&self) {
        self.entries.write().await.clear();
        self.listener_hashes.write().await.clear();
        self.name_to_hash.write().await.clear();
    }

    /// Register a name→hash mapping for later lookup via `get_hash_for_name`.
    pub async fn register_name(&self, name: &str, hash: &str) {
        self.name_to_hash
            .write()
            .await
            .insert(name.to_string(), hash.to_string());
    }

    /// Get the address-hash hex string for a registered name.
    ///
    /// Returns `Some(hash)` if the name was registered via `register_name`,
    /// or `None` if unknown.
    pub async fn get_hash_for_name(&self, name: &str) -> Option<String> {
        self.name_to_hash.read().await.get(name).cloned()
    }
}

static STORE: once_cell::sync::OnceCell<Arc<HandleStore>> = once_cell::sync::OnceCell::new();

pub fn global_store() -> &'static Arc<HandleStore> {
    STORE.get_or_init(HandleStore::new)
}
