use reticulum_rs::transport::destination::link::Link;
use reticulum_rs::transport::hash::AddressHash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Maximum plaintext bytes per `data_packet()` call.
/// Derived from `PACKET_MDU (464) - Fernet overhead (IV 16 + HMAC 32 + AES padding 16) = 400`.
const LXMF_MAX_PAYLOAD: usize = 400;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Connection {
    id: u64,
    inner: ConnectionInner,
}

#[derive(Clone)]
enum ConnectionInner {
    /// Real Reticulum link connection.
    Link {
        link: Arc<Mutex<Link>>,
        link_id: AddressHash,
        /// Ephemeral link identity hash of the remote peer (from `link.peer_identity()`).
        peer_hash: Option<AddressHash>,
        /// Verified persistent transport identity hash of the remote peer,
        /// obtained from a `LinkIdentify` (0xFB) exchange after link activation.
        identified_peer: Option<AddressHash>,
    },
    /// In-memory buffered connection — test use only.
    #[cfg(test)]
    Memory {
        write_buf: Arc<tokio::sync::RwLock<Vec<u8>>>,
    },
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            ConnectionInner::Link { link_id, .. } => f
                .debug_struct("Connection")
                .field("id", &self.id)
                .field("variant", &"Link")
                .field("link_id", link_id)
                .finish(),
            #[cfg(test)]
            ConnectionInner::Memory { .. } => f
                .debug_struct("Connection")
                .field("id", &self.id)
                .field("variant", &"Memory")
                .finish(),
        }
    }
}

impl Connection {
    /// Create a connection wrapping a real Reticulum Link.
    pub fn new_from_link(
        link: Arc<Mutex<Link>>,
        link_id: AddressHash,
        peer_hash: Option<AddressHash>,
        identified_peer: Option<AddressHash>,
    ) -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            inner: ConnectionInner::Link {
                link,
                link_id,
                peer_hash,
                identified_peer,
            },
        }
    }

    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            inner: ConnectionInner::Memory {
                write_buf: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            },
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Write data to the connection via the Reticulum link.
    ///
    /// Small payloads (≤ LXMF_MAX_PAYLOAD) are sent as a single `data_packet` for low latency.
    /// Larger payloads are sent via the Resource protocol, which handles splitting and
    /// retransmission internally and supports up to 64 MB.
    pub async fn write(&self, data: &[u8]) -> Result<usize, String> {
        match &self.inner {
            ConnectionInner::Link { link, link_id, .. } => {
                if data.len() <= LXMF_MAX_PAYLOAD {
                    let (packet, iface) = {
                        let link_guard = link.lock().await;
                        let packet = match link_guard.data_packet(data) {
                            Ok(p) => p,
                            Err(e) => return Err(format!("{:?}", e)),
                        };
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
                } else {
                    let transport = crate::transport::get_transport()
                        .ok_or_else(|| "transport not initialized".to_string())?;
                    let tp = transport.lock().await;
                    tp.send_resource(link_id, data.to_vec(), None)
                        .await
                        .map(|_hash| data.len())
                        .map_err(|e| format!("send_resource: {:?}", e))
                }
            }
            #[cfg(test)]
            ConnectionInner::Memory { write_buf } => {
                let mut buf = write_buf.write().await;
                buf.extend_from_slice(data);
                Ok(data.len())
            }
        }
    }

    pub fn link_id(&self) -> Option<AddressHash> {
        match &self.inner {
            ConnectionInner::Link { link_id, .. } => Some(*link_id),
            #[cfg(test)]
            ConnectionInner::Memory { .. } => None,
        }
    }

    /// Returns the address hash of the remote peer as seen by `link.peer_identity()`.
    pub fn peer_hash(&self) -> Option<AddressHash> {
        match &self.inner {
            ConnectionInner::Link { peer_hash, .. } => *peer_hash,
            #[cfg(test)]
            ConnectionInner::Memory { .. } => None,
        }
    }

    /// Returns the verified persistent transport identity hash of the remote peer.
    pub fn identified_peer(&self) -> Option<AddressHash> {
        match &self.inner {
            ConnectionInner::Link {
                identified_peer, ..
            } => *identified_peer,
            #[cfg(test)]
            ConnectionInner::Memory { .. } => None,
        }
    }

    pub fn link(&self) -> Option<Arc<Mutex<Link>>> {
        match &self.inner {
            ConnectionInner::Link { link, .. } => Some(link.clone()),
            #[cfg(test)]
            ConnectionInner::Memory { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_write_memory() {
        let conn = Connection::new();
        let data = b"hello world";
        let written = conn.write(data).await.unwrap();
        assert_eq!(written, data.len());
    }

    #[tokio::test]
    async fn test_connection_write_memory_small_uses_buffer() {
        // Small writes (≤ LXMF_MAX_PAYLOAD) go to the write buffer in Memory variant.
        let conn = Connection::new();
        let data = vec![0xAB; LXMF_MAX_PAYLOAD];
        let written = conn.write(&data).await.unwrap();
        assert_eq!(written, LXMF_MAX_PAYLOAD);
    }

    #[tokio::test]
    async fn test_connection_write_memory_large_uses_buffer() {
        // Memory variant has no size distinction — it always writes to buffer.
        // This confirms the Memory branch doesn't panic or reject large writes.
        let conn = Connection::new();
        let data = vec![0xCD; LXMF_MAX_PAYLOAD + 1];
        let written = conn.write(&data).await.unwrap();
        assert_eq!(written, LXMF_MAX_PAYLOAD + 1);
    }

    #[tokio::test]
    async fn test_connection_id_unique() {
        let a = Connection::new();
        let b = Connection::new();
        assert_ne!(a.id(), b.id());
        assert!(a.id() > 0);
    }

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
        let conn = Connection::new_from_link(link.clone(), link_id, None, None);
        assert_eq!(conn.link_id(), Some(link_id));
        assert!(conn.link().is_some());
        assert!(conn.peer_hash().is_none());
        assert!(conn.identified_peer().is_none());
    }

    #[tokio::test]
    async fn test_connection_with_hashes() {
        use reticulum_rs::transport::identity::PrivateIdentity;
        let (link, link_id) = make_test_link();
        let peer_id = PrivateIdentity::new_from_rand(rand_core::OsRng);
        let peer_hash = *peer_id.address_hash();
        let conn = Connection::new_from_link(link, link_id, Some(peer_hash), Some(peer_hash));
        assert_eq!(conn.peer_hash(), Some(peer_hash));
        assert_eq!(conn.identified_peer(), Some(peer_hash));
    }
}
