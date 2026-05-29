use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use reticulum_rs::transport::destination::link::Link;
use reticulum_rs::transport::hash::AddressHash;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// A Reticulum connection. Write-only from Rust's perspective — inbound data
/// flows through the on_data callback registered at reticulum_init.
#[derive(Clone)]
pub struct Connection {
    id:        u64,
    link:      Arc<Mutex<Link>>,
    link_id:   AddressHash,
    peer_hash: String,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("link_id", &self.link_id)
            .field("peer_hash", &self.peer_hash)
            .finish()
    }
}

impl Connection {
    pub fn new_from_link(link: Arc<Mutex<Link>>, link_id: AddressHash, peer_hash: String) -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            link,
            link_id,
            peer_hash,
        }
    }

    pub fn id(&self) -> u64 { self.id }

    pub fn link(&self) -> &Arc<Mutex<Link>> { &self.link }

    pub fn link_id(&self) -> AddressHash { self.link_id }

    pub fn peer_hash(&self) -> &str { &self.peer_hash }

    /// Send data to the remote end.
    pub async fn write(&self, data: &[u8]) -> Result<usize, String> {
        let (packet, iface) = {
            let link_guard = self.link.lock().await;
            let status = link_guard.status();
            log::warn!(
                "conn.write: conn={} link={} status={:?} len={}",
                self.id, self.link_id, status, data.len()
            );
            let packet = link_guard.data_packet(data)
                .map_err(|e| format!("{:?}", e))?;
            (packet, link_guard.ingress_iface())
        };
        if let Some(transport) = crate::transport::get_transport() {
            let tp = transport.lock().await;
            if let Some(iface) = iface {
                tp.send_direct(iface, packet).await;
            } else {
                tp.send_broadcast(packet, None).await;
            }
        }
        Ok(data.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_link() -> (Arc<Mutex<Link>>, AddressHash) {
        use reticulum_rs::transport::destination::DestinationDesc;
        use reticulum_rs::transport::identity::PrivateIdentity;
        use tokio::sync::broadcast;

        let identity = PrivateIdentity::new_from_rand(rand_core::OsRng);
        let pub_identity = *identity.as_identity();
        let desc = DestinationDesc {
            identity: pub_identity,
            address_hash: *identity.address_hash(),
            name: reticulum_rs::transport::destination::DestinationName::new("test", "test"),
        };
        let (tx, _) = broadcast::channel(16);
        let link = Arc::new(Mutex::new(Link::new(desc, tx)));
        let link_id = *identity.address_hash();
        (link, link_id)
    }

    #[tokio::test]
    async fn test_connection_new_from_link() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id, "abc123".to_string());
        assert!(conn.id() > 0);
        assert_eq!(conn.link_id(), link_id);
        assert_eq!(conn.peer_hash(), "abc123");
    }

    #[tokio::test]
    async fn test_connection_id_unique() {
        let (link1, id1) = make_test_link();
        let (link2, id2) = make_test_link();
        let c1 = Connection::new_from_link(link1, id1, "a".to_string());
        let c2 = Connection::new_from_link(link2, id2, "b".to_string());
        assert_ne!(c1.id(), c2.id());
    }
}
