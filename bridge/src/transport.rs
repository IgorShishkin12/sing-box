//! Global Transport singleton backed by `reticulum-rs` real networking.
//!
//! IMPORTANT: `reticulum_rs` re-exports `rns_core` types at the top level
//! (e.g. `reticulum_rs::destination`, `reticulum_rs::identity`, `reticulum_rs::hash`)
//! and `rns_transport` types under `reticulum_rs::transport::*`. These are *distinct
//! types* even when they have the same name. The `Transport` API uses `rns_transport`
//! types, so all imports here must reference `reticulum_rs::transport::*` sub-modules.

use once_cell::sync::OnceCell;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::{broadcast, watch};

use rand_core::OsRng;
use reticulum_rs::resource::{ResourceEvent, ResourceEventKind};
use reticulum_rs::runtime::ReceivedData;
use reticulum_rs::transport::destination::link::{Link, LinkEvent, LinkEventData, LinkStatus};
use reticulum_rs::transport::destination::{
    DestinationDesc, DestinationName, SingleInputDestination,
};
use reticulum_rs::transport::hash::AddressHash;
use reticulum_rs::transport::identity::{Identity, PrivateIdentity};
use reticulum_rs::transport::iface::tcp_client::TcpClient;
use reticulum_rs::transport::iface::tcp_server::TcpServer;
use reticulum_rs::transport::iface::udp::UdpInterface;
use reticulum_rs::transport::iface::InterfaceManager;
use reticulum_rs::transport::transport::{Transport, TransportConfig};
use reticulum_rs::transport::PacketContext;

use crate::config::{ReticulumConfig, ReticulumInterface};
use crate::listener::Listener;
use crate::runtime;

/// Maximum time to wait for a link to become active during dial.
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for link activation.
const DIAL_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Interval between service re-announces.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Transport singletons (resettable for shutdown/reinit support)
// ---------------------------------------------------------------------------

static TRANSPORT: OnceCell<StdMutex<Option<Arc<Mutex<Transport>>>>> = OnceCell::new();
static TRANSPORT_IDENTITY: OnceCell<StdMutex<Option<Arc<PrivateIdentity>>>> = OnceCell::new();
static TRANSPORT_IDENTITY_HASH: OnceCell<StdMutex<Option<AddressHash>>> = OnceCell::new();

fn transport_store() -> &'static StdMutex<Option<Arc<Mutex<Transport>>>> {
    TRANSPORT.get_or_init(|| StdMutex::new(None))
}

fn identity_store() -> &'static StdMutex<Option<Arc<PrivateIdentity>>> {
    TRANSPORT_IDENTITY.get_or_init(|| StdMutex::new(None))
}

fn identity_hash_store() -> &'static StdMutex<Option<AddressHash>> {
    TRANSPORT_IDENTITY_HASH.get_or_init(|| StdMutex::new(None))
}

/// Get a clone of the global Transport Arc, if initialized.
pub fn get_transport() -> Option<Arc<Mutex<Transport>>> {
    transport_store()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Returns the hex-encoded transport identity address hash, or `None` before init.
pub fn get_transport_identity_hash() -> Option<String> {
    identity_hash_store()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .map(|h| h.to_hex_string())
}

/// Returns a clone of the transport-level private identity Arc, or `None` before init.
pub fn get_transport_identity() -> Option<Arc<PrivateIdentity>> {
    identity_store()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Clear all transport singletons so the next `init_transport` call starts fresh.
/// Called by `reticulum_shutdown`.
pub fn clear_transport() {
    *transport_store().lock().unwrap_or_else(|p| p.into_inner()) = None;
    *identity_store().lock().unwrap_or_else(|p| p.into_inner()) = None;
    *identity_hash_store()
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = None;
    log::info!("transport singletons cleared");
}

// ---------------------------------------------------------------------------
// Link identify helpers (mirrors Python RNS link.identify())
// ---------------------------------------------------------------------------

/// Build an identify payload for sending on a link.
///
/// Format (128 bytes total):
///   [encrypt_key: 32][sign_key: 32][sig_over(link_id||encrypt_key||sign_key): 64]
///
/// The signature binds the identity claim to this specific link_id, preventing
/// replay on a different link.
pub fn build_link_identify_payload(identity: &PrivateIdentity, link_id: &AddressHash) -> Vec<u8> {
    let id = identity.as_identity();
    let mut keys = Vec::with_capacity(64);
    keys.extend_from_slice(id.public_key_bytes());
    keys.extend_from_slice(id.verifying_key_bytes());

    let mut signed_data = Vec::with_capacity(16 + 64);
    signed_data.extend_from_slice(link_id.as_slice());
    signed_data.extend_from_slice(&keys);
    let sig = identity.sign(&signed_data);

    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(&keys);
    payload.extend_from_slice(&sig.to_bytes());
    payload
}

/// Parse and verify an identify payload received on a link.
///
/// Returns the verified `Identity` on success, or `None` if the payload is
/// malformed or the signature does not verify.
pub fn parse_link_identify_payload(payload: &[u8], link_id: &AddressHash) -> Option<Identity> {
    if payload.len() < 128 {
        return None;
    }
    let encrypt_key = &payload[..32];
    let sign_key = &payload[32..64];
    let sig_bytes: &[u8; 64] = payload[64..128].try_into().ok()?;

    let identity = Identity::new_from_slices(encrypt_key, sign_key);

    let mut signed_data = Vec::with_capacity(16 + 64);
    signed_data.extend_from_slice(link_id.as_slice());
    signed_data.extend_from_slice(&payload[..64]);

    if !reticulum_rs::transport::identity::lxmf_verify(&identity, &signed_data, sig_bytes) {
        return None;
    }
    Some(identity)
}

// ---------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------

/// Load a persisted service identity from `<config_dir>/<name>-service.key`,
/// or create and save a new random one if the file does not exist.
///
/// The identity hex is in the same format as `PrivateIdentity::to_hex_string` /
/// `new_from_hex_string` (128 hex chars = 64 bytes private key material).
pub fn load_or_create_service_identity(
    config_dir: &str,
    name: &str,
) -> Result<PrivateIdentity, String> {
    let key_path = std::path::PathBuf::from(config_dir).join(format!("{}-service.key", name));

    if key_path.exists() {
        let mut file = std::fs::File::open(&key_path).map_err(|e| {
            format!(
                "failed to open identity key '{}': {}",
                key_path.display(),
                e
            )
        })?;
        let mut hex = String::new();
        file.read_to_string(&mut hex).map_err(|e| {
            format!(
                "failed to read identity key '{}': {}",
                key_path.display(),
                e
            )
        })?;
        PrivateIdentity::new_from_hex_string(hex.trim())
            .map_err(|_| format!("invalid identity key in '{}'", key_path.display()))
    } else {
        let identity = PrivateIdentity::new_from_rand(OsRng);
        let hex = identity.to_hex_string();
        let mut file = std::fs::File::create(&key_path).map_err(|e| {
            format!(
                "failed to create identity key '{}': {}",
                key_path.display(),
                e
            )
        })?;
        file.write_all(hex.as_bytes()).map_err(|e| {
            format!(
                "failed to write identity key '{}': {}",
                key_path.display(),
                e
            )
        })?;
        log::info!(
            "created new service identity for '{}': addr={}",
            name,
            identity.address_hash()
        );
        Ok(identity)
    }
}

/// Resolve the transport-level identity from config.
///
/// Priority:
/// 1. `identity_key` → load directly from hex string.
/// 2. `identity_name` → load or create a persisted random identity in `config_dir`.
/// 3. Neither → generate an ephemeral random identity (not persisted).
fn resolve_identity(cfg: &ReticulumConfig, config_dir: &str) -> Result<PrivateIdentity, String> {
    if let Some(ref key) = cfg.identity_key {
        PrivateIdentity::new_from_hex_string(key)
            .map_err(|_| "invalid identity_key hex string".to_string())
    } else if let Some(ref name) = cfg.identity_name {
        load_or_create_service_identity(config_dir, name)
    } else {
        log::warn!("no identity specified; using ephemeral random identity");
        Ok(PrivateIdentity::new_from_rand(OsRng))
    }
}

/// Determine the Reticulum config directory path from config.
///
/// Checks `config_dir` first, then falls back to `storage_path` (legacy alias),
/// then `$HOME/.reticulum`, then `/etc/reticulum`.
pub fn config_dir_path(cfg: &ReticulumConfig) -> String {
    if let Some(ref dir) = cfg.config_dir {
        return dir.clone();
    }
    if let Some(ref path) = cfg.storage_path {
        return path.clone();
    }
    if let Ok(home) = std::env::var("HOME") {
        return format!("{}/.reticulum", home);
    }
    "/etc/reticulum".to_string()
}

// ---------------------------------------------------------------------------
// Interface spawning
// ---------------------------------------------------------------------------

/// Spawn network interfaces from the config list.
///
/// AutoInterface uses `Transport::add_multicast_udp_interface` which registers
/// a proper PeerRouting map so point-to-point replies to discovered peers are
/// delivered as unicast on the same socket rather than being re-broadcast.
///
/// All other interface types lock the InterfaceManager arc per-spawn.
async fn spawn_interfaces(
    transport: &Transport,
    iface_mgr: Arc<tokio::sync::Mutex<InterfaceManager>>,
    interfaces: &[ReticulumInterface],
) {
    let default = vec![ReticulumInterface {
        name: Some("Default Interface".to_string()),
        iface_type: "AutoInterface".to_string(),
        ..Default::default()
    }];
    let ifaces = if interfaces.is_empty() {
        log::info!("no interfaces configured, using AutoInterface default");
        default.as_slice()
    } else {
        interfaces
    };

    for iface in ifaces {
        let label = iface.name.as_deref().unwrap_or(&iface.iface_type);
        match iface.iface_type.as_str() {
            "AutoInterface" => {
                let port = iface.data_port.unwrap_or(49555);
                let mcast = format!("239.255.0.1:{port}");
                let addr = transport
                    .add_multicast_udp_interface(mcast.clone(), Some(mcast))
                    .await;
                log::info!(
                    "spawned AutoInterface '{}' multicast 239.255.0.1:{} addr={}",
                    label,
                    port,
                    addr
                );
            }
            "UDPInterface" => {
                let lip = iface.listen_ip.as_deref().unwrap_or("0.0.0.0");
                let lport = iface.listen_port.unwrap_or(4242);
                let bind = format!("{lip}:{lport}");
                let fwd = iface
                    .forward_ip
                    .as_ref()
                    .map(|ip| format!("{}:{}", ip, iface.forward_port.unwrap_or(lport)));
                let ui = UdpInterface::new(&bind, fwd.as_ref());
                let addr = iface_mgr.lock().await.spawn(ui, UdpInterface::spawn);
                log::info!(
                    "spawned UDPInterface '{}' bind={} forward={:?} addr={}",
                    label,
                    bind,
                    fwd,
                    addr
                );
            }
            "TCPServerInterface" => {
                let lip = iface.listen_ip.as_deref().unwrap_or("0.0.0.0");
                let lport = iface.listen_port.unwrap_or(7788);
                let bind = format!("{lip}:{lport}");
                let ts = TcpServer::new(&bind, iface_mgr.clone());
                let addr = iface_mgr.lock().await.spawn(ts, TcpServer::spawn);
                log::info!(
                    "spawned TCPServerInterface '{}' bind={} addr={}",
                    label,
                    bind,
                    addr
                );
            }
            "TCPClientInterface" => {
                let host = iface.target_host.as_deref().unwrap_or("");
                let port = iface.target_port.unwrap_or(7788);
                let target = format!("{host}:{port}");
                let tc = TcpClient::new(&target);
                let addr = iface_mgr.lock().await.spawn(tc, TcpClient::spawn);
                log::info!(
                    "spawned TCPClientInterface '{}' target={} addr={}",
                    label,
                    target,
                    addr
                );
            }
            other => {
                log::warn!("unknown interface type '{}', skipping", other);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transport initialization
// ---------------------------------------------------------------------------

/// Initialize the global Transport singleton with the given config.
/// Safe to call after `clear_transport()` — will re-initialize cleanly.
pub fn init_transport(cfg: &ReticulumConfig) -> Result<(), String> {
    if transport_store()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .is_some()
    {
        log::debug!("Transport already initialized");
        return Ok(());
    }

    // 1. Determine config directory and create it
    let cfg_dir = config_dir_path(cfg);
    if let Err(e) = std::fs::create_dir_all(&cfg_dir) {
        log::error!("failed to create config dir '{}': {}", cfg_dir, e);
        return Err(e.to_string());
    }
    log::info!("config dir: {}", cfg_dir);

    // 2. Resolve transport-level identity (persisted)
    let identity = match resolve_identity(cfg, &cfg_dir) {
        Ok(id) => id,
        Err(e) => {
            log::error!("identity resolution failed: {}", e);
            return Err(e);
        }
    };
    {
        let id = identity.as_identity();
        log::info!(
            "transport identity: addr={} encrypt_key={} sign_key={}",
            id.address_hash,
            hex::encode(id.public_key_bytes()),
            hex::encode(id.verifying_key_bytes()),
        );
    }
    let identity_hash = *identity.address_hash();

    // 3. Build ratchet store path
    let ratchet_store = Some(std::path::PathBuf::from(&cfg_dir).join("ratchet_store.db"));

    // 4. Build TransportConfig
    let mut tp_config = TransportConfig::new("sing-box-reticulum", &identity, true);
    tp_config.set_broadcast(true);
    tp_config.set_retransmit(true);
    if let Some(ref rstore) = ratchet_store {
        tp_config.set_ratchet_store_path(rstore.clone());
    }

    // 5. Create Transport and spawn interfaces (no transport lock held during block_on)
    let interfaces = cfg.interfaces.clone();
    log::debug!("about to block_on for Transport::new");
    let transport = runtime::block_on(async move {
        log::debug!("inside block_on, creating Transport");
        let transport = Transport::new(tp_config);
        log::debug!("Transport created, spawning interfaces");

        let iface_mgr = transport.iface_manager();
        spawn_interfaces(&transport, iface_mgr, &interfaces).await;
        log::debug!("interfaces spawned");

        transport
    });
    log::info!("Transport initialized successfully");

    // 6. Store — re-check under lock to handle concurrent init races
    {
        let mut guard = transport_store().lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_some() {
            log::debug!("Transport initialized concurrently, discarding duplicate");
            return Ok(());
        }
        *guard = Some(Arc::new(Mutex::new(transport)));
    }
    *identity_store().lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(identity));
    *identity_hash_store()
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(identity_hash);
    Ok(())
}

// ---------------------------------------------------------------------------
// Service destination (random, persisted identity)
// ---------------------------------------------------------------------------

/// Register a service `SingleInputDestination` with the global Transport.
///
/// Spawns a background task that monitors `in_link_events` and fires
/// `call_on_accept(listener_handle, conn_id, peer_hash)` for each incoming
/// link whose destination hash matches this service dest.
///
/// Returns `(address_hash, Arc<Mutex<SingleInputDestination>>)`.
pub async fn register_listener_destination(
    listener_handle: u64,
    listener: Arc<Listener>,
    identity: PrivateIdentity,
    app_name: String,
    aspect: String,
) -> Result<(AddressHash, Arc<Mutex<SingleInputDestination>>), &'static str> {
    let transport = get_transport().ok_or("Transport not initialized")?;

    let (address_hash, destination, mut link_events) = {
        let mut tp = transport.lock().await;
        let name = DestinationName::new(&app_name, &aspect);
        let dest = tp.add_destination(identity, name).await;
        let hash = {
            let d = dest.lock().await;
            d.desc.address_hash
        };
        let events = tp.in_link_events();
        (hash, dest, events)
    };

    log::info!(
        "registered service destination: addr={} app={} aspect={}",
        address_hash,
        app_name,
        aspect
    );

    let store = crate::store::global_store();
    let service_hash = address_hash;

    let task = tokio::spawn(async move {
        loop {
            match link_events.recv().await {
                Ok(event) => {
                    if !matches!(event.event, LinkEvent::Activated) {
                        continue;
                    }
                    let transport = match get_transport() {
                        Some(t) => t,
                        None => break,
                    };

                    let link = {
                        let tp = transport.lock().await;
                        tp.find_in_link(&event.id).await
                    };

                    if let Some(link) = link {
                        let link_dest_hash = {
                            let guard = link.lock().await;
                            guard.destination().address_hash
                        };

                        if link_dest_hash == service_hash {
                            let peer_hash = {
                                let guard = link.lock().await;
                                guard.peer_identity().address_hash
                            };
                            log::info!(
                                "service link activated: id={} peer={}",
                                event.id,
                                peer_hash
                            );
                            let mut peer_events = {
                                let tp = transport.lock().await;
                                tp.in_link_events()
                            };
                            let data_rx = {
                                let tp = transport.lock().await;
                                tp.received_data_events()
                            };
                            let identified =
                                exchange_identify_on_link(&link, event.id, &mut peer_events).await;
                            let conn = crate::connection::Connection::new_from_link(
                                link.clone(),
                                event.id,
                                Some(peer_hash),
                                identified,
                            );
                            let conn_id = store.insert_connection(conn).await;
                            spawn_link_data_reader(conn_id, event.id, data_rx);
                            let resource_rx = {
                                let tp = transport.lock().await;
                                tp.resource_events()
                            };
                            spawn_resource_event_reader(conn_id, event.id, resource_rx);
                            crate::c_api::call_on_accept(
                                listener_handle,
                                conn_id,
                                &peer_hash.to_hex_string(),
                            );
                            // Keep listener alive; suppress unused-var warning.
                            let _ = &listener;
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    log::warn!("service link event channel closed");
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("service link event channel lagged by {}", n);
                    continue;
                }
            }
        }
    });
    runtime::register_task(task);

    Ok((address_hash, destination))
}

// ---------------------------------------------------------------------------
// Service announcement
// ---------------------------------------------------------------------------

/// Send service destination announcements in a loop.
///
/// Announces every `ANNOUNCE_INTERVAL` seconds until `stop_rx` receives `true`
/// or the sender is explicitly dropped with a stop signal. There is no iteration
/// cap — the caller controls lifetime via the watch channel.
pub async fn start_service_announce_loop(
    service_dest: Arc<Mutex<SingleInputDestination>>,
    name: String,
    mut stop_rx: watch::Receiver<bool>,
) {
    loop {
        if *stop_rx.borrow() {
            break;
        }
        if let Some(tp) = get_transport() {
            tp.lock()
                .await
                .send_announce(&service_dest, Some(name.as_bytes()))
                .await;
            log::debug!("announced service dest for name='{}'", name);
        }
        tokio::select! {
            result = stop_rx.changed() => {
                // Stop if signaled true; ignore sender-dropped errors (keep running).
                if result.is_ok() && *stop_rx.borrow() {
                    break;
                }
                // Sender dropped without sending true — sleep to avoid busy loop.
                tokio::time::sleep(ANNOUNCE_INTERVAL).await;
            }
            _ = tokio::time::sleep(ANNOUNCE_INTERVAL) => {}
        }
    }
    log::info!("service announce loop finished for name='{}'", name);
}

/// Wait for an announce from the network where `app_data == name`.
///
/// Returns the hex-encoded service destination address hash on success, or
/// `None` if the timeout elapses without a matching announce.
pub async fn wait_for_service_announce(name: &str, timeout: Duration) -> Option<String> {
    let transport = get_transport()?;
    let mut announces = {
        let tp = transport.lock().await;
        tp.recv_announces().await
    };
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        tokio::select! {
            _ = tokio::time::sleep(remaining) => return None,
            result = announces.recv() => {
                match result {
                    Ok(event) if event.app_data.as_slice() == name.as_bytes() => {
                        let dest = event.destination.lock().await;
                        let hash = dest.desc.address_hash.to_hex_string();
                        log::info!(
                            "received service announce for '{}': hash={}",
                            name, hash
                        );
                        return Some(hash);
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Data reader
// ---------------------------------------------------------------------------

/// Exchange link identify packets with the peer and return the peer's verified
/// transport identity hash. Both sides are expected to call this concurrently
/// right after link activation.
///
/// The library handles `PacketContext::LinkIdentify` (0xFB) internally and fires
/// `LinkEvent::PeerIdentified` — it never appears in `received_data_events`.
/// Therefore this function listens on the transport link-event channel, not the
/// data channel. Callers must subscribe to `in_link_events` (server) or
/// `out_link_events` (client) **before** the link activates so that the event
/// is buffered and not missed.
///
/// Logs:
///   my_identity:   addr=… encrypt=… sign=…
///   peer_identity: addr=… encrypt=… sign=…
///
/// Returns `None` if the local identity is unavailable, the identify packet
/// cannot be sent, or the peer's identify does not arrive within 5 seconds.
pub async fn exchange_identify_on_link(
    link: &Arc<Mutex<Link>>,
    link_id: AddressHash,
    link_events: &mut broadcast::Receiver<LinkEventData>,
) -> Option<AddressHash> {
    let transport_id = get_transport_identity()?;

    let my_id = transport_id.as_identity();
    log::info!(
        "my_identity: addr={} encrypt={} sign={}",
        my_id.address_hash,
        hex::encode(my_id.public_key_bytes()),
        hex::encode(my_id.verifying_key_bytes()),
    );

    // Send our identify packet using PacketContext::LinkIdentify (0xFB).
    let payload = build_link_identify_payload(&transport_id, &link_id);
    let (packet, iface) = {
        let guard = link.lock().await;
        let pkt = match guard.data_packet(&payload) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("identify: failed to build packet: {:?}", e);
                return None;
            }
        };
        (pkt, guard.ingress_iface())
    };
    if let Some(tp) = get_transport() {
        let tp = tp.lock().await;
        if let Some(iface) = iface {
            tp.send_direct(iface, packet).await;
        } else {
            tp.send_broadcast(packet, None).await;
        }
    }

    // Wait for the library to fire LinkEvent::PeerIdentified on this link.
    // The library decrypts the peer's LinkIdentify packet internally and posts
    // this event — it does NOT forward the raw packet to received_data_events.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            log::warn!(
                "identify: timeout waiting for peer identify on link {}",
                link_id
            );
            return None;
        }
        tokio::select! {
            _ = tokio::time::sleep(remaining) => {
                log::warn!("identify: timeout waiting for peer identify on link {}", link_id);
                return None;
            }
            result = link_events.recv() => {
                match result {
                    Ok(event) if event.id == link_id => {
                        if let LinkEvent::PeerIdentified(identity) = event.event {
                            log::info!(
                                "peer_identity: addr={} encrypt={} sign={}",
                                identity.address_hash,
                                hex::encode(identity.public_key_bytes()),
                                hex::encode(identity.verifying_key_bytes()),
                            );
                            return Some(identity.address_hash);
                        }
                        // Other events on this link (e.g. Data, KeepAlive) — skip.
                    }
                    Ok(_) => {} // event for a different link — skip
                    Err(broadcast::error::RecvError::Closed) => return None,
                    Err(broadcast::error::RecvError::Lagged(_)) => {} // catch up
                }
            }
        }
    }
}

/// Spawn a background task that fires `on_data` / `on_close` callbacks for
/// inbound link data packets.
///
/// `data_rx` must be subscribed to `received_data_events` **before** the link
/// is created so no packets are lost to the broadcast-channel race.
pub fn spawn_link_data_reader(
    conn_id: u64,
    link_id: AddressHash,
    mut data_rx: broadcast::Receiver<ReceivedData>,
) {
    let handle = tokio::spawn(async move {
        loop {
            match data_rx.recv().await {
                Ok(data) => {
                    if data.destination == link_id
                        && data.context != Some(PacketContext::LinkIdentify)
                    {
                        crate::c_api::call_on_data(conn_id, data.data.as_slice());
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::warn!("data event channel closed for conn {}", conn_id);
                    crate::c_api::call_on_close(conn_id);
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    });
    runtime::register_task(handle);
}

/// Spawn a background task that fires `on_data` for inbound Resource completions.
///
/// When the peer sends a payload too large for a single `data_packet` it uses the
/// Reticulum Resource protocol. The transport delivers the fully-reassembled payload
/// as a `ResourceComplete` event. We forward it to Go via the same `on_data` callback
/// so the mux layer sees a single contiguous TypeLargeData message.
pub fn spawn_resource_event_reader(
    conn_id: u64,
    link_id: AddressHash,
    mut resource_rx: broadcast::Receiver<ResourceEvent>,
) {
    let handle = tokio::spawn(async move {
        loop {
            match resource_rx.recv().await {
                Ok(event) if event.link_id == link_id => match event.kind {
                    ResourceEventKind::Complete(complete) => {
                        log::trace!(
                            "resource complete: conn={} link={} len={}",
                            conn_id,
                            link_id,
                            complete.data.len()
                        );
                        crate::c_api::call_on_data(conn_id, &complete.data);
                    }
                    ResourceEventKind::Progress(ref p) => {
                        log::trace!(
                            "resource inbound progress: conn={} link={} received={} total={}",
                            conn_id,
                            link_id,
                            p.received_bytes,
                            p.total_bytes
                        );
                    }
                    _ => {}
                },
                Ok(event) => {
                    log::trace!(
                        "resource event for other link (ignored): our={} event_link={}",
                        link_id,
                        event.link_id
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!(
                        "resource inbound event channel lagged {} events: conn={} link={}",
                        n,
                        conn_id,
                        link_id
                    );
                    continue;
                }
            }
        }
    });
    runtime::register_task(handle);
}

// ---------------------------------------------------------------------------
// Outbound dial (service destination, hash known from announce)
// ---------------------------------------------------------------------------

/// Resolve a destination hash string to an `AddressHash`.
fn parse_dest_hash(hex_str: &str) -> Result<AddressHash, &'static str> {
    let clean = hex_str
        .strip_prefix("rln://")
        .or_else(|| hex_str.strip_prefix("0x"))
        .unwrap_or(hex_str);
    AddressHash::new_from_hex_string(clean).map_err(|_| "invalid destination hash hex string")
}

/// Dial a remote service destination by its hash string.
///
/// Requires the destination's identity to already be in the transport's
/// announce table (populated after receiving the server's service announce).
pub async fn dial_and_wait(
    dest_hash: &str,
) -> Result<(Arc<Mutex<Link>>, AddressHash), &'static str> {
    let transport = get_transport().ok_or("Transport not initialized")?;

    let address_hash = parse_dest_hash(dest_hash)?;

    let identity = {
        let tp = transport.lock().await;
        tp.destination_identity(&address_hash)
            .await
            .ok_or("unknown destination: identity not found in announce table")?
    };

    let mut link_events = {
        let tp = transport.lock().await;
        tp.out_link_events()
    };

    let name = DestinationName::new("sing-box-reticulum", "dial");
    let desc = DestinationDesc {
        identity,
        address_hash,
        name,
    };

    let link = {
        let tp = transport.lock().await;
        tp.link(desc).await
    };

    log::info!("initiated link request to {}", address_hash);

    let link_clone = link.clone();
    let link_id = *link.lock().await.id();
    let start = tokio::time::Instant::now();

    loop {
        if start.elapsed() >= DIAL_TIMEOUT {
            log::warn!("dial timeout for link {}", link_id);
            return Err("dial timed out waiting for link activation");
        }

        let link_status = { link_clone.lock().await.status() };
        match link_status {
            LinkStatus::Active => {
                log::info!("link {} active, dial successful", link_id);
                return Ok((link_clone, link_id));
            }
            LinkStatus::Closed | LinkStatus::Stale => {
                log::error!("link {} failed with status {:?}", link_id, link_status);
                return Err("link failed before becoming active");
            }
            _ => {}
        }

        // TODO: replace sleep+try_recv with tokio::select! { link_events.recv() ... } so
        // activation is event-driven instead of polled every 100 ms. Blocked on confirming
        // that the transport library emits a Closed/Failed LinkEvent (needed to avoid
        // hanging until DIAL_TIMEOUT on silent link failure).
        tokio::time::sleep(DIAL_POLL_INTERVAL).await;
        match link_events.try_recv() {
            Ok(event) => {
                if event.id == link_id && matches!(event.event, LinkEvent::Activated) {
                    log::info!("link {} activated (event), dial successful", link_id);
                    return Ok((link_clone, link_id));
                }
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                log::error!("link event channel closed while dialing");
                return Err("link event channel closed");
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dest_hash_with_rln_prefix() {
        let hash = "rln://aabbccdd00112233445566778899aabb";
        let result = parse_dest_hash(hash);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().to_hex_string(),
            "aabbccdd00112233445566778899aabb"
        );
    }

    #[test]
    fn test_parse_dest_hash_invalid_hex() {
        let result = parse_dest_hash("not-hex-string");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid destination hash hex string");
    }

    #[test]
    fn test_parse_dest_hash_plain_hex() {
        let hex_str = "aabbccdd00112233445566778899aabb";
        let result = parse_dest_hash(hex_str);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().to_hex_string(), hex_str);
    }

    #[test]
    fn test_parse_dest_hash_too_short() {
        let result = parse_dest_hash("aabbccdd00112233445566778899aa");
        assert!(result.is_err());
    }

    #[test]
    fn test_dial_and_wait_no_transport() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(dial_and_wait("rln://aabbccdd00112233445566778899aabb"));
        assert!(result.is_err());
        match result {
            Err(msg) => assert_eq!(msg, "Transport not initialized"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn test_identity_persistence() {
        let dir = std::env::temp_dir().join(format!("test-identity-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg_dir = dir.to_str().unwrap();

        let id1 = load_or_create_service_identity(cfg_dir, "test-service").unwrap();
        let id2 = load_or_create_service_identity(cfg_dir, "test-service").unwrap();

        assert_eq!(id1.to_hex_string(), id2.to_hex_string());

        // A different name gets a different identity.
        let id3 = load_or_create_service_identity(cfg_dir, "other-service").unwrap();
        assert_ne!(id1.to_hex_string(), id3.to_hex_string());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_identity_key_override() {
        use rand_core::OsRng;

        let dir = std::env::temp_dir().join(format!("test-identity-key-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg_dir = dir.to_str().unwrap();

        let original = PrivateIdentity::new_from_rand(OsRng);
        let hex = original.to_hex_string();

        let cfg = ReticulumConfig {
            identity_key: Some(hex.clone()),
            identity_name: None,
            config_dir: Some(cfg_dir.to_string()),
            storage_path: None,
            interfaces: vec![],
            identity_path: None,
            reticulum_config_path: None,
        };

        let loaded = resolve_identity(&cfg, cfg_dir).unwrap();
        assert_eq!(original.to_hex_string(), loaded.to_hex_string());

        // No service key file should have been written.
        let key_file = dir.join("test-service-service.key");
        assert!(!key_file.exists());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_wait_for_announce_no_transport() {
        // Without an initialized transport, wait_for_service_announce returns None immediately.
        let result = wait_for_service_announce("nonexistent", Duration::from_millis(50)).await;
        assert!(result.is_none());
    }

    /// spawn_resource_event_reader exits cleanly when the sender is dropped.
    #[tokio::test]
    async fn test_resource_event_reader_exits_on_channel_close() {
        use reticulum_rs::resource::ResourceEvent;
        use reticulum_rs::transport::hash::AddressHash;
        use tokio::sync::broadcast;

        let (tx, rx) = broadcast::channel::<ResourceEvent>(16);
        let target_link =
            AddressHash::new_from_hex_string("aabbccdd00112233445566778899aabb").unwrap();
        spawn_resource_event_reader(99, target_link, rx);
        // Drop the sender — the reader task must exit cleanly (no hang, no panic).
        drop(tx);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    /// spawn_resource_event_reader ignores events from a different link.
    /// Exercises the link_id filter without touching the ON_DATA callback slot.
    #[tokio::test]
    async fn test_resource_event_reader_filters_by_link_id() {
        use reticulum_rs::resource::{ResourceComplete, ResourceEvent, ResourceEventKind};
        use reticulum_rs::transport::hash::{AddressHash, Hash};
        use tokio::sync::broadcast;

        // Use a broadcast channel to simulate transport events.
        let (tx, rx) = broadcast::channel::<ResourceEvent>(16);
        let target_link =
            AddressHash::new_from_hex_string("aabbccdd00112233445566778899aabb").unwrap();
        let other_link =
            AddressHash::new_from_hex_string("1111222233334444555566667777aaaa").unwrap();

        // Use a second receiver to verify events are dispatched.
        // We can't inject into ON_DATA easily, so we verify via a second broadcast
        // subscriber that the events we send are the ones that would be forwarded.
        let mut monitor_rx = tx.subscribe();

        spawn_resource_event_reader(2, target_link, rx);

        // Send an event for a different link.
        tx.send(ResourceEvent {
            hash: Hash::new_from_slice(&[0u8; 32]),
            link_id: other_link,
            kind: ResourceEventKind::Complete(ResourceComplete {
                data: b"not for us".to_vec(),
                metadata: None,
                request_id: None,
                is_request: false,
                is_response: false,
            }),
        })
        .unwrap();

        // Send an event for our link.
        tx.send(ResourceEvent {
            hash: Hash::new_from_slice(&[0u8; 32]),
            link_id: target_link,
            kind: ResourceEventKind::Complete(ResourceComplete {
                data: b"for us".to_vec(),
                metadata: None,
                request_id: None,
                is_request: false,
                is_response: false,
            }),
        })
        .unwrap();

        // Verify two events were published total (reader receives both but filters one).
        let ev1 = monitor_rx.recv().await.unwrap();
        let ev2 = monitor_rx.recv().await.unwrap();
        assert_eq!(ev1.link_id, other_link);
        assert_eq!(ev2.link_id, target_link);

        drop(tx);
    }
}
