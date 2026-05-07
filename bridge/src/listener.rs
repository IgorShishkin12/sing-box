use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "real-reticulum")]
use std::sync::Mutex;
#[cfg(feature = "real-reticulum")]
use reticulum_rs::transport::destination::SingleInputDestination;
#[cfg(feature = "real-reticulum")]
use reticulum_rs::transport::hash::AddressHash;

use crate::connection::Connection;

static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(1);

/// A listener that accepts incoming connections.
///
/// When the `real-reticulum` feature is active, the listener may also hold
/// a reference to a registered `SingleInputDestination` in the global Transport,
/// and its accept queue is populated by a background link event subscriber.
pub struct Listener {
    id: u64,
    hash: Option<String>,
    accept_queue: Arc<RwLock<Vec<Connection>>>,
    /// Real Reticulum destination (only used when real-reticulum feature is active).
    #[cfg(feature = "real-reticulum")]
    destination: Option<Arc<Mutex<SingleInputDestination>>>,
    /// The address hash of the registered destination.
    /// Uses Arc<RwLock> so it can be set after creation via Arc reference.
    #[cfg(feature = "real-reticulum")]
    destination_hash: Arc<RwLock<Option<AddressHash>>>,
}

// Manual Debug impl to avoid requiring Debug on all generic fields.
// We implement it conditionally for both feature sets.
impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(not(feature = "real-reticulum"))]
        {
            f.debug_struct("Listener")
                .field("id", &self.id)
                .field("hash", &self.hash)
                .field("accept_queue", &self.accept_queue)
                .finish()
        }
        #[cfg(feature = "real-reticulum")]
        {
            f.debug_struct("Listener")
                .field("id", &self.id)
                .field("hash", &self.hash)
                .field("accept_queue", &self.accept_queue)
                .field("destination", &self.destination.as_ref().map(|_| "Some(...)"))
                .field("destination_hash", &self.destination_hash)
                .finish()
        }
    }
}

// Manual Clone impl to avoid requiring Clone on SingleInputDestination.
impl Clone for Listener {
    fn clone(&self) -> Self {
        #[cfg(not(feature = "real-reticulum"))]
        {
            Self {
                id: self.id,
                hash: self.hash.clone(),
                accept_queue: self.accept_queue.clone(),
            }
        }
        #[cfg(feature = "real-reticulum")]
        {
            Self {
                id: self.id,
                hash: self.hash.clone(),
                accept_queue: self.accept_queue.clone(),
                destination: self.destination.clone(),
                destination_hash: self.destination_hash.clone(),
            }
        }
    }
}

impl Listener {
    pub fn new() -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash: None,
            accept_queue: Arc::new(RwLock::new(Vec::new())),
            #[cfg(feature = "real-reticulum")]
            destination: None,
            #[cfg(feature = "real-reticulum")]
            destination_hash: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_hash(hash: String) -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash: Some(hash),
            accept_queue: Arc::new(RwLock::new(Vec::new())),
            #[cfg(feature = "real-reticulum")]
            destination: None,
            #[cfg(feature = "real-reticulum")]
            destination_hash: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a listener with a real Reticulum destination.
    #[cfg(feature = "real-reticulum")]
    pub fn with_destination(
        hash: String,
        destination: Arc<Mutex<SingleInputDestination>>,
        destination_hash: AddressHash,
    ) -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash: Some(hash),
            accept_queue: Arc::new(RwLock::new(Vec::new())),
            destination: Some(destination),
            destination_hash: Arc::new(RwLock::new(Some(destination_hash))),
        }
    }

    pub fn hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the real Reticulum destination, if any.
    #[cfg(feature = "real-reticulum")]
    pub fn destination(&self) -> Option<&Arc<Mutex<SingleInputDestination>>> {
        self.destination.as_ref()
    }

    /// Get the destination address hash, if any.
    #[cfg(feature = "real-reticulum")]
    pub async fn destination_hash(&self) -> Option<AddressHash> {
        *self.destination_hash.read().await
    }

    /// Set the destination address hash after creation (works through Arc).
    #[cfg(feature = "real-reticulum")]
    pub async fn set_destination_hash(&self, hash: AddressHash) {
        *self.destination_hash.write().await = Some(hash);
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

    #[cfg(feature = "real-reticulum")]
    #[tokio::test]
    async fn test_listener_with_destination() {
        use reticulum_rs::transport::destination::SingleInputDestination;
        use reticulum_rs::transport::destination::DestinationName;
        use reticulum_rs::transport::identity::PrivateIdentity;
        use std::sync::Mutex;

        let identity = PrivateIdentity::new_from_rand(rand_core::OsRng);
        let dest = Arc::new(Mutex::new(SingleInputDestination::new(
            identity.clone(),
            DestinationName::new("bridge-test", "listen"),
        )));
        let dest_hash = *identity.address_hash();

        let listener = Listener::with_destination("test-hash".to_string(), dest.clone(), dest_hash);
        assert_eq!(listener.hash(), Some("test-hash"));
        assert_eq!(listener.destination_hash().await, Some(dest_hash));
        assert!(listener.destination().is_some());
    }
}