use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Connection {
    id: u64,
    read_buf: Arc<RwLock<Vec<u8>>>,
    write_buf: Arc<RwLock<Vec<u8>>>,
}

impl Connection {
    pub fn new() -> Self {
        let shared = Arc::new(RwLock::new(Vec::new()));
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            read_buf: shared.clone(),
            write_buf: shared,
        }
    }

    fn new_split(read_buf: Arc<RwLock<Vec<u8>>>, write_buf: Arc<RwLock<Vec<u8>>>) -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
            read_buf,
            write_buf,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Write data to the connection's write buffer (data sent to peer).
    pub async fn write(&self, data: &[u8]) -> usize {
        let mut buf = self.write_buf.write().await;
        buf.extend_from_slice(data);
        data.len()
    }

    /// Read data from the connection's read buffer (data received from peer).
    /// Returns the number of bytes read.
    pub async fn read(&self, buf: &mut [u8]) -> usize {
        let mut read_buf = self.read_buf.write().await;
        if read_buf.is_empty() {
            return 0;
        }
        let len = buf.len().min(read_buf.len());
        buf[..len].copy_from_slice(&read_buf[..len]);
        read_buf.drain(..len);
        len
    }

    /// Read all available data without removing it from the buffer.
    pub async fn peek(&self) -> Vec<u8> {
        self.read_buf.read().await.clone()
    }

    /// Get the number of bytes available to read.
    pub async fn available(&self) -> usize {
        self.read_buf.read().await.len()
    }
}

/// Create a pair of connected connections.
/// Data written to one is readable from the other, and vice versa.
pub async fn create_pair() -> (Arc<Connection>, Arc<Connection>) {
    // Each direction gets its own buffer:
    //   conn_a  → buf_ab →  conn_b
    //   conn_b  → buf_ba →  conn_a
    let buf_ab = Arc::new(RwLock::new(Vec::new()));
    let buf_ba = Arc::new(RwLock::new(Vec::new()));

    let conn_a = Arc::new(Connection::new_split(
        buf_ba.clone(), // conn_a reads what conn_b writes
        buf_ab.clone(), // conn_a writes to buf_ab → conn_b reads
    ));
    let conn_b = Arc::new(Connection::new_split(
        buf_ab.clone(), // conn_b reads what conn_a writes
        buf_ba.clone(), // conn_b writes to buf_ba → conn_a reads
    ));

    (conn_a, conn_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_write_read() {
        let conn = Connection::new();
        let data = b"hello world";
        let written = conn.write(data).await;
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
        conn.write(b"test").await;
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
        conn.write(b"abc").await;
        assert_eq!(conn.available().await, 3);
        let mut buf = [0u8; 2];
        conn.read(&mut buf).await;
        assert_eq!(conn.available().await, 1);
    }

    #[tokio::test]
    async fn test_create_pair_roundtrip() {
        let (a, b) = create_pair().await;

        // a writes → b should read
        let written = a.write(b"hello").await;
        assert_eq!(written, 5);
        assert_eq!(b.available().await, 5);
        let mut buf = [0u8; 5];
        let read = b.read(&mut buf).await;
        assert_eq!(read, 5);
        assert_eq!(&buf, b"hello");

        // b writes → a should read
        b.write(b"world").await;
        assert_eq!(a.available().await, 5);
        let mut buf = [0u8; 5];
        let read = a.read(&mut buf).await;
        assert_eq!(read, 5);
        assert_eq!(&buf, b"world");
    }
}