use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Listener {
    id: u64,
}

impl Listener {
    pub fn new() -> Self {
        Self {
            id: NEXT_LISTENER_ID.fetch_add(1, Ordering::SeqCst),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn accept(&self) -> Option<crate::connection::Connection> {
        // Stub: no incoming connections.
        None
    }
}
