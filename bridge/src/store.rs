use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
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
}

impl HandleStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            listener_hashes: RwLock::new(HashMap::new()),
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
    }

    /// Register a name→hash mapping.
    pub async fn register_name(&self, name: &str, _hash: &str) {
        let mut hashes = self.listener_hashes.write().await;
        hashes.insert(format!("name:{}", name), 0); // Placeholder; real impl in Step 12 uses Reticulum identity
    }

    /// Get the hash for a registered name. Returns None if unknown.
    pub async fn get_hash_for_name(&self, name: &str) -> Option<String> {
        let hashes = self.listener_hashes.read().await;
        // For now, we store hash lookups separately. In Step 12, this resolves via reticulum-rs.
        let key = format!("name:{}", name);
        if hashes.contains_key(&key) {
            // Return a deterministic hash based on the name
            let mut hasher = DefaultHasher::new();
            name.hash(&mut hasher);
            Some(format!("{:016x}", hasher.finish()))
        } else {
            None
        }
    }
}

static STORE: once_cell::sync::OnceCell<Arc<HandleStore>> = once_cell::sync::OnceCell::new();

pub fn global_store() -> &'static Arc<HandleStore> {
    STORE.get_or_init(HandleStore::new)
}
