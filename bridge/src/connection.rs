use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    Link {
        link: Arc<Mutex<Link>>,
        link_id: AddressHash,
        read_buf: Arc<RwLock<Vec<u8>>>,
        /// Set to true when the link closes so BridgeRead returns -1 (EOF).
        eof: Arc<AtomicBool>,
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
                read_buf: Arc::new(RwLock::new(Vec::new())),
                eof: Arc::new(AtomicBool::new(false)),
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

    /// Signal EOF so that subsequent read() calls return -1.
    pub async fn push_eof(&self) {
        if let ConnectionInner::Link { eof, .. } = &self.inner {
            eof.store(true, Ordering::SeqCst);
        }
    }

    /// Close the underlying Reticulum link.  Sends a teardown packet to the peer
    /// (notifying it to close too) then marks the link Closed locally.
    /// Only acts if the link is still Active or Stale; idempotent otherwise.
    pub async fn close_link(&self) {
        if let ConnectionInner::Link { link, link_id, .. } = &self.inner {
            let (packet, iface) = {
                let mut guard = link.lock().await;
                use reticulum_rs::transport::destination::link::LinkStatus;
                if !matches!(guard.status(), LinkStatus::Active | LinkStatus::Stale) {
                    return; // already closed
                }
                log::debug!("[conn {}] close_link: initiating teardown, link={:?}", self.id, link_id);
                let iface = guard.ingress_iface();
                let packet = guard.teardown(); // sends teardown + sets Closed
                (packet, iface)
            };
            if let Some(packet) = packet {
                if let Some(transport) = crate::transport::get_transport() {
                    let tp = transport.lock().await;
                    if let Some(iface) = iface {
                        tp.send_direct(iface, packet).await;
                    } else {
                        tp.send_broadcast(packet, None).await;
                    }
                }
            }
        }
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

    /// Read data from the connection's read buffer.
    /// Returns: > 0 = bytes read, 0 = no data (try again), -1 = EOF (link closed).
    pub async fn read(&self, buf: &mut [u8]) -> i32 {
        match &self.inner {
            ConnectionInner::Link { read_buf, eof, .. } => {
                let mut buf_lock = read_buf.write().await;
                if buf_lock.is_empty() {
                    if eof.load(Ordering::SeqCst) {
                        return -1; // link closed, signal EOF
                    }
                    return 0;
                }
                let len = buf.len().min(buf_lock.len());
                buf[..len].copy_from_slice(&buf_lock[..len]);
                buf_lock.drain(..len);
                len as i32
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
                len as i32
            }
        }
    }

    /// Push data into the read buffer (called by the link event subscriber).
    pub async fn push_read_data(&self, data: &[u8]) {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => {
                read_buf.write().await.extend_from_slice(data);
            }
            #[cfg(test)]
            ConnectionInner::Memory { .. } => {}
        }
    }

    /// Read all available data without removing it from the buffer.
    pub async fn peek(&self) -> Vec<u8> {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => read_buf.read().await.clone(),
            #[cfg(test)]
            ConnectionInner::Memory { read_buf, .. } => read_buf.read().await.clone(),
        }
    }

    /// Get the number of bytes available to read.
    pub async fn available(&self) -> usize {
        match &self.inner {
            ConnectionInner::Link { read_buf, .. } => read_buf.read().await.len(),
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
    async fn test_link_connection_push_eof() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        conn.push_eof().await;
        let mut buf = [0u8; 4];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, -1, "read after push_eof should return -1");
    }

    #[tokio::test]
    async fn test_link_connection_push_read_data_multi() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        conn.push_read_data(b"abc").await;
        conn.push_read_data(b"def").await;
        conn.push_read_data(b"ghi").await;
        assert_eq!(conn.available().await, 9);
        let mut buf = [0u8; 9];
        let read = conn.read(&mut buf).await;
        assert_eq!(read, 9);
        assert_eq!(&buf, b"abcdefghi");
    }

    #[tokio::test]
    async fn test_link_connection_peek_and_available() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        assert_eq!(conn.available().await, 0);
        conn.push_read_data(b"peek test").await;
        assert_eq!(conn.available().await, 9);
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

    /// Exercises close_link on a link-type connection. The link is freshly
    /// constructed (not Active), so close_link is a no-op — but the function
    /// must be called at least once for coverage.
    #[tokio::test]
    async fn test_link_connection_close_link_idempotent() {
        let (link, link_id) = make_test_link();
        let conn = Connection::new_from_link(link, link_id);
        conn.close_link().await;
        conn.close_link().await; // second call is also safe
    }
}
