use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connection::Connection;

static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Listener {
    id: u64,
    accept_queue: Arc<RwLock<Vec<Connection>>>,
}

impl Listener {
    pub fn new() -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            accept_queue: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Accept a pending connection, if any.
    pub async fn accept(&self) -> Option<Connection> {
        let mut queue = self.accept_queue.write().await;
        if queue.is_empty() {
            None
        } else {
            Some(queue.remove(0))
        }
    }

    /// Add a new connection to the accept queue (simulating an incoming connection).
    pub async fn push_connection(&self, conn: Connection) {
        let mut queue = self.accept_queue.write().await;
        queue.push(conn);
    }

    /// Get the number of pending connections.
    pub async fn pending(&self) -> usize {
        self.accept_queue.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_listener_accept_empty() {
        let listener = Listener::new();
        let conn = listener.accept().await;
        assert!(conn.is_none());
    }

    #[tokio::test]
    async fn test_listener_push_and_accept() {
        let listener = Listener::new();
        let conn = Connection::new();
        listener.push_connection(conn).await;

        assert_eq!(listener.pending().await, 1);
        let accepted = listener.accept().await;
        assert!(accepted.is_some());
        assert_eq!(listener.pending().await, 0);
    }

    #[tokio::test]
    async fn test_listener_multiple_accept() {
        let listener = Listener::new();
        listener.push_connection(Connection::new()).await;
        listener.push_connection(Connection::new()).await;
        listener.push_connection(Connection::new()).await;

        assert_eq!(listener.pending().await, 3);
        assert!(listener.accept().await.is_some());
        assert!(listener.accept().await.is_some());
        assert!(listener.accept().await.is_some());
        assert!(listener.accept().await.is_none());
        assert_eq!(listener.pending().await, 0);
    }
}
