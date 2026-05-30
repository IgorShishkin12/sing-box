use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use reticulum_rs::transport::destination::link::Link;
use reticulum_rs::transport::hash::AddressHash;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Connection {
    id: u64,
    inner: ConnectionInner,
}

#[derive(Clone)]
enum ConnectionInner {
    /// Real Reticulum link connection.
    /// read_buf holds one Vec<u8> per received data_packet, preserving message boundaries.
    Link {
        link: Arc<Mutex<Link>>,
        link_id: AddressHash,
        read_buf: Arc<RwLock<VecDeque<Vec<u8>>>>,
    },
    /// In-memory buffered connection — test use only.
    #[cfg(test)]
    Memory {
        read_buf: Arc<RwLock<Vec<u8>>>,
        write_buf: Arc<RwLock<Vec<u8>>>,
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
    pub fn new_from_link(link: Arc<Mutex<Link>>, link_id: AddressHash) -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            inner: ConnectionInner::Link {
                link,
                link_id,
                read_buf: Arc::new(RwLock::new(VecDeque::new())),
            },
        }
    }

    #[cfg(test)]
    pub fn new() -> Self {
        let shared = Arc::new(RwLock::new(Vec::new()));
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            inner: ConnectionInner::Memory {
                read_buf: shared.clone(),
                write_buf: shared,
            },
        }
    }

    #[cfg(test)]
    fn new_split(read_buf: Arc<RwLock<Vec<u8>>>, write_buf: Arc<RwLock<Vec<u8>>>) -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            inner: ConnectionInner::Memory { read_buf, write_buf },
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Write data to the connection.
    /// For a Link connection: sends via Link::data_packet; always writes all bytes on success.
    pub async fn write(&self, data: &[u8]) -> Result<usize, String> {
        match &self.inner {
            ConnectionInner::Link { link, .. } => {
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
            }
            #[cfg(test)]
            ConnectionInner::Memory { write_buf, .. } => {
                let mut buf = write_buf.write().await;
                buf.extend_from_slice(data);
                Ok(data.len())
            }
        }
    }

    /// Read exactly one complete message from the connection's read buffer.
    ///
    /// For Link connections the buffer stores one Vec<u8> per received
    /// data_packet, so this returns exactly one packet's payload per call.
    /// Returns 0 if no complete message is available yet.
    pub async fn read(&self, buf: &mut [u8]) -> usize {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => {
                let mut deque = read_buf.write().await;
                match deque.front_mut() {
                    None => 0,
                    Some(msg) => {
                        let copy_len = buf.len().min(msg.len());
                        buf[..copy_len].copy_from_slice(&msg[..copy_len]);
                        if copy_len >= msg.len() {
                            deque.pop_front();
                        } else {
                            // Partial read: drain the consumed prefix.
                            msg.drain(..copy_len);
                        }
                        copy_len
                    }
                }
            }
            #[cfg(test)]
            ConnectionInner::Memory { read_buf, .. } => {
                let mut read_buf = read_buf.write().await;
                if read_buf.is_empty() {
                    return 0;
                }
                let len = buf.len().min(read_buf.len());
                buf[..len].copy_from_slice(&read_buf[..len]);
                read_buf.drain(..len);
                len
            }
        }
    }

    /// Push one complete message into the read buffer (called by the link event subscriber).
    /// Each call corresponds to exactly one received data_packet, preserving boundaries.
    pub async fn push_read_data(&self, data: &[u8]) {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => {
                read_buf.write().await.push_back(data.to_vec());
            }
            #[cfg(test)]
            ConnectionInner::Memory { .. } => {}
        }
    }

    /// Peek at the first available message without removing it from the buffer.
    pub async fn peek(&self) -> Vec<u8> {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => {
                read_buf.read().await.front().cloned().unwrap_or_default()
            }
            #[cfg(test)]
            ConnectionInner::Memory { read_buf, .. } => read_buf.read().await.clone(),
        }
    }

    /// Get the number of bytes in the first available message (0 if no message is queued).
    pub async fn available(&self) -> usize {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => {
                read_buf.read().await.front().map_or(0, |m| m.len())
            }
            #[cfg(test)]
            ConnectionInner::Memory { read_buf, .. } => read_buf.read().await.len(),
        }
    }

    pub fn link_id(&self) -> Option<AddressHash> {
        match &self.inner {
            ConnectionInner::Link { link_id, .. } => Some(*link_id),
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

/// Create a pair of connected in-memory connections — test use only.
/// Data written to one is readable from the other, and vice versa.
#[cfg(test)]
pub async fn create_pair() -> (Arc<Connection>, Arc<Connection>) {
    let buf_ab = Arc::new(RwLock::new(Vec::new()));
    let buf_ba = Arc::new(RwLock::new(Vec::new()));
    let conn_a = Arc::new(Connection::new_split(buf_ba.clone(), buf_ab.clone()));
    let conn_b = Arc::new(Connection::new_split(buf_ab.clone(), buf_ba.clone()));
    (conn_a, conn_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_write_read() {
        let conn = Connection::new();
        let data = b"hello world";
        let written = conn.write(data).await.unwrap();
        assert_eq!(written, data.len());

        let mut buf = [0u8; 11];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 11);
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_connection_read_empty() {
        let conn = Connection::new();
        let mut buf = [0u8; 4];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 0);
    }

    #[tokio::test]
    async fn test_connection_peek() {
        let conn = Connection::new();
        conn.write(b"test").await.unwrap();
        let peeked = conn.peek().await;
        assert_eq!(peeked, b"test");
        let mut buf = [0u8; 4];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 4);
    }

    #[tokio::test]
    async fn test_connection_available() {
        let conn = Connection::new();
        assert_eq!(conn.available().await, 0);
        conn.write(b"abc").await.unwrap();
        assert_eq!(conn.available().await, 3);
        let mut buf = [0u8; 2];
        conn.read(&mut buf).await;
        assert_eq!(conn.available().await, 1);
    }

    #[tokio::test]
    async fn test_create_pair_roundtrip() {
        let (a, b) = create_pair().await;

        let written = a.write(b"hello").await.unwrap();
        assert_eq!(written, 5);
        assert_eq!(b.available().await, 5);
        let mut buf = [0u8; 5];
        let read = b.read(&mut buf).await;
        assert_eq!(read, 5);
        assert_eq!(&buf, b"hello");

        b.write(b"world").await.unwrap();
        assert_eq!(a.available().await, 5);
        let mut buf = [0u8; 5];
        let read = a.read(&mut buf).await;
        assert_eq!(read, 5);
        assert_eq!(&buf, b"world");
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
        let conn = Connection::new_from_link(link.clone(), link_id);
        assert_eq!(conn.link_id(), Some(link_id));
        assert!(conn.link().is_some());
    }

    #[tokio::test]
    async fn test_link_connection_push_read_data() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        conn.push_read_data(b"hello link").await;
        let mut buf = [0u8; 10];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 10);
        assert_eq!(&buf, b"hello link");
    }

    #[tokio::test]
    async fn test_link_connection_push_read_data_multi() {
        // Each push_read_data call is a discrete message; read() returns one message at a time.
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        conn.push_read_data(b"abc").await;
        conn.push_read_data(b"def").await;
        conn.push_read_data(b"ghi").await;
        // available() reports the size of the first queued message.
        assert_eq!(conn.available().await, 3);
        let mut buf = [0u8; 9];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 3);
        assert_eq!(&buf[..3], b"abc");
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 3);
        assert_eq!(&buf[..3], b"def");
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 3);
        assert_eq!(&buf[..3], b"ghi");
    }

    #[tokio::test]
    async fn test_link_connection_peek_and_available() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        assert_eq!(conn.available().await, 0);
        conn.push_read_data(b"peek test").await;
        // available() returns the size of the first queued message.
        assert_eq!(conn.available().await, 9);
        // peek() returns a clone of the first queued message without removing it.
        let peeked = conn.peek().await;
        assert_eq!(peeked, b"peek test");
        assert_eq!(conn.available().await, 9);
    }

    #[tokio::test]
    async fn test_link_connection_read_empty() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        let mut buf = [0u8; 4];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 0);
    }

    #[tokio::test]
    async fn test_link_connection_id() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        assert!(conn.id() > 0);
        assert_eq!(conn.link_id(), Some(link_id));
    }
}
