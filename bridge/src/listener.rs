use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use reticulum_rs::transport::hash::AddressHash;

static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(1);

/// A named service listener. Accepted connections are delivered via the
/// on_accept callback registered at reticulum_init — there is no queue here.
#[derive(Debug)]
pub struct Listener {
    id:               u64,
    hash:             Option<String>,
    destination_hash: RwLock<Option<AddressHash>>,
}

impl Clone for Listener {
    fn clone(&self) -> Self {
        Self {
            id:               self.id,
            hash:             self.hash.clone(),
            destination_hash: RwLock::new(
                // best-effort synchronous snapshot; only used in store cloning
                self.destination_hash.try_read().ok()
                    .and_then(|g| *g),
            ),
        }
    }
}

impl Listener {
    pub fn with_hash(hash: String) -> Self {
        Self {
            id:               NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
            hash:             Some(hash),
            destination_hash: RwLock::new(None),
        }
    }

    pub fn id(&self) -> u64 { self.id }

    pub fn hash(&self) -> Option<&str> { self.hash.as_deref() }

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
    async fn test_listener_with_hash() {
        let l = Listener::with_hash("test-service".to_string());
        assert_eq!(l.hash(), Some("test-service"));
        assert!(l.destination_hash().await.is_none());
    }

    #[tokio::test]
    async fn test_listener_set_destination_hash() {
        use reticulum_rs::transport::identity::PrivateIdentity;
        let l = Listener::with_hash("svc".to_string());
        let identity = PrivateIdentity::new_from_rand(rand_core::OsRng);
        let hash = *identity.address_hash();
        l.set_destination_hash(hash).await;
        assert_eq!(l.destination_hash().await, Some(hash));
    }

    #[test]
    fn test_listener_ids_unique() {
        let l1 = Listener::with_hash("a".to_string());
        let l2 = Listener::with_hash("b".to_string());
        assert_ne!(l1.id(), l2.id());
    }
}
