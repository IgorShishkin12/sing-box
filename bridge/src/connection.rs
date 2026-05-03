use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Connection {
    id: u64,
}

impl Connection {
    pub fn new() -> Self {
        Self {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn write(&self, data: &[u8]) -> usize {
        // Stub: pretend we wrote all data.
        data.len()
    }

    pub fn read(&self, _buf: &mut [u8]) -> usize {
        // Stub: no data available.
        0
    }
}
