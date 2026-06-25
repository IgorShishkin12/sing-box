use reticulum_rs::transport::crypt::fernet::{FERNET_MAX_PADDING_SIZE, FERNET_OVERHEAD_SIZE};
use reticulum_rs::resource::{ResourceEvent, ResourceEventKind};
use reticulum_rs::transport::destination::link::Link;
use reticulum_rs::transport::hash::{AddressHash, Hash};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Wait on `resource_rx` until the outbound resource transfer `resource_hash` succeeds,
/// fails, is cancelled, or the inactivity deadline expires.
///
/// Extracted from `Connection::write` so it can be unit-tested without a full transport.
pub(crate) async fn wait_for_outbound_resource(
    conn_id: u64,
    resource_hash: Hash,
    data_len: usize,
    mut resource_rx: broadcast::Receiver<ResourceEvent>,
) -> Result<usize, String> {
    // Inactivity timeout for the outbound resource transfer. The *total* transfer
    // time is unbounded (a large body over a slow link can legitimately take many
    // minutes), so this must never cap total duration — it is reset on every
    // progress event reporting more bytes acknowledged by the peer. Only genuine
    // silence (peer stopped acking for this long) aborts the send. Kept generous
    // so it defers to the lib's own progress-aware retry handling.
    const INACTIVITY_SECS: u64 = 180;
    const HEARTBEAT_SECS: u64 = 15;
    let mut last_progress_bytes: u64 = 0;
    let started = tokio::time::Instant::now();
    let mut deadline = started + Duration::from_secs(INACTIVITY_SECS);
    let mut last_activity = started;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await; // consume the immediate first tick
    log::debug!(
        "[res-bridge] awaiting outbound resource conn={} hash={} data_len={} inactivity_timeout={}s",
        conn_id, resource_hash, data_len, INACTIVITY_SECS
    );
    loop {
        tokio::select! {
            // Periodic heartbeat so the client is never silent during a long/slow
            // transfer: prints elapsed time, bytes the peer has acked, and how long
            // since the last progress (so a stall is visible well before it aborts).
            _ = heartbeat.tick() => {
                log::info!(
                    "[res-bridge] outbound resource in progress conn={} hash={} acked={}/{} elapsed={:.0}s since_progress={:.0}s (aborts after {}s silence)",
                    conn_id, resource_hash, last_progress_bytes, data_len,
                    started.elapsed().as_secs_f32(),
                    last_activity.elapsed().as_secs_f32(), INACTIVITY_SECS,
                );
                continue;
            }
            _ = tokio::time::sleep_until(deadline) => {
                log::warn!(
                    "resource send timed out ({}s inactivity): conn={} hash={} data_len={} acked={}",
                    INACTIVITY_SECS, conn_id, resource_hash, data_len, last_progress_bytes
                );
                return Err(format!(
                    "resource inactivity timeout: conn={} hash={} data_len={}",
                    conn_id, resource_hash, data_len
                ));
            }
            result = resource_rx.recv() => {
                match result {
                    Ok(ResourceEvent {
                        hash,
                        kind: ResourceEventKind::OutboundComplete,
                        ..
                    }) if hash == resource_hash => {
                        log::debug!(
                            "resource outbound complete: conn={} hash={}",
                            conn_id, resource_hash
                        );
                        return Ok(data_len);
                    }
                    Ok(ResourceEvent {
                        hash,
                        kind: ResourceEventKind::OutboundFailed,
                        ..
                    }) if hash == resource_hash => {
                        log::warn!(
                            "resource transfer failed (OutboundFailed): conn={} hash={} data_len={}",
                            conn_id, resource_hash, data_len
                        );
                        return Err(format!(
                            "resource OutboundFailed: conn={} hash={}",
                            conn_id, resource_hash
                        ));
                    }
                    Ok(ResourceEvent {
                        hash,
                        kind: ResourceEventKind::OutboundCancelled,
                        ..
                    }) if hash == resource_hash => {
                        log::warn!(
                            "resource transfer cancelled (OutboundCancelled): conn={} hash={} data_len={}",
                            conn_id, resource_hash, data_len
                        );
                        return Err(format!(
                            "resource OutboundCancelled: conn={} hash={}",
                            conn_id, resource_hash
                        ));
                    }
                    Ok(ResourceEvent {
                        kind: ResourceEventKind::Progress(ref p),
                        ..
                    }) => {
                        log::debug!(
                            "[res-bridge] outbound progress conn={} received={}/{} parts={}/{}",
                            conn_id, p.received_bytes, p.total_bytes, p.received_parts, p.total_parts
                        );
                        if p.received_bytes > last_progress_bytes {
                            last_progress_bytes = p.received_bytes;
                            last_activity = tokio::time::Instant::now();
                            deadline = last_activity + Duration::from_secs(INACTIVITY_SECS);
                            log::debug!(
                                "[res-bridge] outbound inactivity deadline reset conn={} acked_bytes={}",
                                conn_id, p.received_bytes
                            );
                        }
                        continue;
                    }
                    Ok(ev) => {
                        log::debug!(
                            "resource event unhandled: conn={} waiting_for={} event={:?}",
                            conn_id, resource_hash, ev
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err("resource event channel closed".to_string());
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!(
                            "resource event channel lagged {} events: conn={} hash={}",
                            n, conn_id, resource_hash
                        );
                        continue;
                    }
                }
            }
        }
    }
}

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
    #[allow(clippy::new_without_default)]
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
    /// Small payloads (≤ `link.packet_mdu() - FERNET_OVERHEAD - FERNET_PADDING`) are sent as a
    /// single `data_packet` for low latency. Larger payloads are sent via the Resource protocol,
    /// which handles splitting and retransmission internally and supports up to 64 MB.
    pub async fn write(&self, data: &[u8]) -> Result<usize, String> {
        match &self.inner {
            ConnectionInner::Link { link, link_id, .. } => {
                let maybe_packet_and_iface = {
                    let link_guard = link.lock().await;
                    let max_plain = link_guard
                        .packet_mdu()
                        .saturating_sub(FERNET_OVERHEAD_SIZE + FERNET_MAX_PADDING_SIZE);
                    if data.len() <= max_plain {
                        let packet = match link_guard.data_packet(data) {
                            Ok(p) => p,
                            Err(e) => return Err(format!("{:?}", e)),
                        };
                        let iface = link_guard.ingress_iface();
                        Some((packet, iface))
                    } else {
                        None
                    }
                };
                if let Some((packet, iface)) = maybe_packet_and_iface {
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
                    // Subscribe before sending so we never miss OutboundComplete
                    // even if the peer acknowledges very quickly.
                    let resource_rx = {
                        let tp = crate::transport::get_transport()
                            .ok_or_else(|| "transport not initialized".to_string())?;
                        let guard = tp.lock().await;
                        guard.resource_events()
                    };
                    let resource_hash = {
                        let tp = crate::transport::get_transport()
                            .ok_or_else(|| "transport not initialized".to_string())?;
                        let guard = tp.lock().await;
                        guard
                            .send_resource(link_id, data.to_vec(), None)
                            .await
                            .map_err(|e| format!("send_resource: {:?}", e))?
                    };
                    log::debug!(
                        "resource outbound start: conn={} link={} hash={} data_len={}",
                        self.id,
                        link_id,
                        resource_hash,
                        data.len()
                    );
                    return wait_for_outbound_resource(
                        self.id,
                        resource_hash,
                        data.len(),
                        resource_rx,
                    )
                    .await;
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
        let conn = Connection::new();
        let data = vec![0xAB; 400];
        let written = conn.write(&data).await.unwrap();
        assert_eq!(written, 400);
    }

    #[tokio::test]
    async fn test_connection_write_memory_large_uses_buffer() {
        // Memory variant has no size distinction — it always writes to buffer.
        let conn = Connection::new();
        let data = vec![0xCD; 401];
        let written = conn.write(&data).await.unwrap();
        assert_eq!(written, 401);
    }

    #[tokio::test]
    async fn test_data_packet_threshold_lora_default() {
        use reticulum_rs::transport::crypt::fernet::{FERNET_MAX_PADDING_SIZE, FERNET_OVERHEAD_SIZE};
        const LORA_MTU: usize = 220;
        let (link, _) = make_test_link();
        let mut link_guard = link.lock().await;
        // Simulate link activation on a LoRa interface (transport calls set_iface_mtu on activation).
        link_guard.set_iface_mtu(LORA_MTU);
        // packet_mdu() = MTU(220) - OVERHEAD(36) = 184
        assert_eq!(link_guard.packet_mdu(), 184);
        let threshold = link_guard
            .packet_mdu()
            .saturating_sub(FERNET_OVERHEAD_SIZE + FERNET_MAX_PADDING_SIZE);
        // 184 - 48 - 16 = 120; matches MaxReticulumMessage on the Go mux side
        assert_eq!(threshold, 120);
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

    // ── wait_for_outbound_resource tests ──────────────────────────────────────

    fn make_resource_hash(b: u8) -> reticulum_rs::transport::hash::Hash {
        reticulum_rs::transport::hash::Hash::new_from_slice(&[b; 32])
    }

    fn make_link_id(b: u8) -> reticulum_rs::transport::hash::AddressHash {
        reticulum_rs::transport::hash::AddressHash::new_from_slice(&[b; 16])
    }

    #[tokio::test]
    async fn test_wait_outbound_resource_complete_returns_ok() {
        use reticulum_rs::resource::{ResourceEvent, ResourceEventKind};

        let (tx, rx) = broadcast::channel::<ResourceEvent>(4);
        let hash = make_resource_hash(0x01);
        tx.send(ResourceEvent {
            hash,
            link_id: make_link_id(0x02),
            kind: ResourceEventKind::OutboundComplete,
        })
        .unwrap();

        let result = wait_for_outbound_resource(1, hash, 500, rx).await;
        assert_eq!(result, Ok(500));
    }

    #[tokio::test]
    async fn test_wait_outbound_resource_failed_returns_err_immediately() {
        use reticulum_rs::resource::{ResourceEvent, ResourceEventKind};

        let (tx, rx) = broadcast::channel::<ResourceEvent>(4);
        let hash = make_resource_hash(0x01);
        tx.send(ResourceEvent {
            hash,
            link_id: make_link_id(0x02),
            kind: ResourceEventKind::OutboundFailed,
        })
        .unwrap();

        let result = wait_for_outbound_resource(1, hash, 500, rx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("OutboundFailed"));
    }

    #[tokio::test]
    async fn test_wait_outbound_resource_cancelled_returns_err_immediately() {
        use reticulum_rs::resource::{ResourceEvent, ResourceEventKind};

        let (tx, rx) = broadcast::channel::<ResourceEvent>(4);
        let hash = make_resource_hash(0x01);
        tx.send(ResourceEvent {
            hash,
            link_id: make_link_id(0x02),
            kind: ResourceEventKind::OutboundCancelled,
        })
        .unwrap();

        let result = wait_for_outbound_resource(1, hash, 500, rx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("OutboundCancelled"));
    }

    #[tokio::test]
    async fn test_wait_outbound_resource_ignores_other_hash() {
        use reticulum_rs::resource::{ResourceEvent, ResourceEventKind};

        let (tx, rx) = broadcast::channel::<ResourceEvent>(4);
        let our_hash = make_resource_hash(0x01);
        let other_hash = make_resource_hash(0xFF);

        // OutboundFailed for a different hash — should NOT cause early return.
        tx.send(ResourceEvent {
            hash: other_hash,
            link_id: make_link_id(0x02),
            kind: ResourceEventKind::OutboundFailed,
        })
        .unwrap();
        // Then OutboundComplete for our hash — should succeed.
        tx.send(ResourceEvent {
            hash: our_hash,
            link_id: make_link_id(0x02),
            kind: ResourceEventKind::OutboundComplete,
        })
        .unwrap();

        let result = wait_for_outbound_resource(1, our_hash, 500, rx).await;
        assert_eq!(result, Ok(500));
    }

    #[tokio::test]
    async fn test_wait_outbound_resource_channel_closed_returns_err() {
        let (tx, rx) = broadcast::channel::<reticulum_rs::resource::ResourceEvent>(4);
        let hash = make_resource_hash(0x01);
        drop(tx);

        let result = wait_for_outbound_resource(1, hash, 500, rx).await;
        assert!(result.is_err());
    }
}
