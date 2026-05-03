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
}

impl HandleStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
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
        self.entries.write().await.remove(&handle)
    }
}

static STORE: once_cell::sync::OnceCell<Arc<HandleStore>> = once_cell::sync::OnceCell::new();

pub fn global_store() -> &'static Arc<HandleStore> {
    STORE.get_or_init(HandleStore::new)
}
