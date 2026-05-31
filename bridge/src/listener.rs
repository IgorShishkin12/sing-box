use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use reticulum_rs::transport::destination::SingleInputDestination;
use reticulum_rs::transport::hash::AddressHash;

static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(1);

pub struct Listener {
    id: u64,
    hash: Option<String>,
    destination: Option<Arc<Mutex<SingleInputDestination>>>,
    destination_hash: Arc<RwLock<Option<AddressHash>>>,
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener")
            .field("id", &self.id)
            .field("hash", &self.hash)
            .field("destination", &self.destination.as_ref().map(|_| "Some(...)"))
            .field("destination_hash", &self.destination_hash)
            .finish()
    }
}

impl Clone for Listener {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            hash: self.hash.clone(),
            destination: self.destination.clone(),
            destination_hash: self.destination_hash.clone(),
        }
    }
}

impl Listener {
    pub fn new() -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash: None,
            destination: None,
            destination_hash: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_hash(hash: String) -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash: Some(hash),
            destination: None,
            destination_hash: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_destination(
        hash: String,
        destination: Arc<Mutex<SingleInputDestination>>,
        destination_hash: AddressHash,
    ) -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash: Some(hash),
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

    pub fn destination(&self) -> Option<&Arc<Mutex<SingleInputDestination>>> {
        self.destination.as_ref()
    }

    pub async fn destination_hash(&self) -> Option<AddressHash> {
        *self.destination_hash.read().await
    }

    pub async fn set_destination_hash(&self, hash: AddressHash) {
        *self.destination_hash.write().await = Some(hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_listener_new() {
        let listener = Listener::new();
        assert!(listener.hash().is_none());
        assert!(listener.destination().is_none());
        assert!(listener.destination_hash().await.is_none());
    }

    #[tokio::test]
    async fn test_listener_with_hash() {
        let listener = Listener::with_hash("test-hash".to_string());
        assert_eq!(listener.hash(), Some("test-hash"));
    }

    #[tokio::test]
    async fn test_listener_set_destination_hash() {
        use reticulum_rs::transport::identity::PrivateIdentity;
        let identity = PrivateIdentity::new_from_rand(rand_core::OsRng);
        let hash = *identity.address_hash();

        let listener = Listener::new();
        assert!(listener.destination_hash().await.is_none());
        listener.set_destination_hash(hash).await;
        assert_eq!(listener.destination_hash().await, Some(hash));
    }

    #[tokio::test]
    async fn test_listener_with_destination() {
        use reticulum_rs::transport::destination::SingleInputDestination;
        use reticulum_rs::transport::destination::DestinationName;
        use reticulum_rs::transport::identity::PrivateIdentity;

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

    #[tokio::test]
    async fn test_listener_id_unique() {
        let a = Listener::new();
        let b = Listener::new();
        assert_ne!(a.id(), b.id());
        assert!(a.id() > 0);
    }
}
