//! Global Transport singleton backed by `reticulum-rs` real networking.
//!
//! IMPORTANT: `reticulum_rs` re-exports `rns_core` types at the top level
//! (e.g. `reticulum_rs::destination`, `reticulum_rs::identity`, `reticulum_rs::hash`)
//! and `rns_transport` types under `reticulum_rs::transport::*`. These are *distinct
//! types* even when they have the same name. The `Transport` API uses `rns_transport`
//! types, so all imports here must reference `reticulum_rs::transport::*` sub-modules.

use once_cell::sync::OnceCell;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::sync::Mutex;

use rand_core::OsRng;
use reticulum_rs::transport::destination::link::{Link, LinkEvent, LinkStatus};
use reticulum_rs::transport::destination::{DestinationDesc, DestinationName, SingleInputDestination};
use reticulum_rs::transport::hash::AddressHash;
use reticulum_rs::transport::identity::{Identity, PrivateIdentity};
use reticulum_rs::transport::iface::tcp_client::TcpClient;
use reticulum_rs::transport::iface::tcp_server::TcpServer;
use reticulum_rs::transport::iface::udp::UdpInterface;
use reticulum_rs::transport::iface::InterfaceManager;
use reticulum_rs::transport::transport::{Transport, TransportConfig};
use reticulum_rs::runtime::ReceivedData;

use crate::config::{ReticulumConfig, ReticulumInterface};
use crate::connection::Connection;
use crate::listener::Listener;
use crate::runtime;
use crate::c_api::{call_on_accept, call_on_data, call_on_close};

/// Maximum time to wait for a link to become active during dial.
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for link activation.
const DIAL_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Interval between service re-announces.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5);

/// Global Transport singleton.
static TRANSPORT: OnceCell<Arc<Mutex<Transport>>> = OnceCell::new();

/// Get a reference to the global Transport, if initialized.
pub fn get_transport() -> Option<&'static Arc<Mutex<Transport>>> {
    TRANSPORT.get()
}

// ---------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------

pub fn derive_discovery_identity(name: &str) -> PrivateIdentity {
    PrivateIdentity::new_from_name(name)
}

pub fn discovery_hash_for_name(name: &str) -> String {
    let identity = derive_discovery_identity(name);
    let dest_name = DestinationName::new("sing-box-reticulum", &format!("discovery.{}", name));
    let dest = SingleInputDestination::new(identity, dest_name);
    dest.desc.address_hash.to_hex_string()
}

pub fn load_or_create_service_identity(config_dir: &str, name: &str) -> Result<PrivateIdentity, String> {
    let key_path = std::path::PathBuf::from(config_dir)
        .join(format!("{}-service.key", name));

    if key_path.exists() {
        let mut file = std::fs::File::open(&key_path)
            .map_err(|e| format!("failed to open identity key '{}': {}", key_path.display(), e))?;
        let mut hex = String::new();
        file.read_to_string(&mut hex)
            .map_err(|e| format!("failed to read identity key '{}': {}", key_path.display(), e))?;
        PrivateIdentity::new_from_hex_string(hex.trim())
            .map_err(|_| format!("invalid identity key in '{}'", key_path.display()))
    } else {
        let identity = PrivateIdentity::new_from_rand(OsRng);
        let hex = identity.to_hex_string();
        let mut file = std::fs::File::create(&key_path)
            .map_err(|e| format!("failed to create identity key '{}': {}", key_path.display(), e))?;
        file.write_all(hex.as_bytes())
            .map_err(|e| format!("failed to write identity key '{}': {}", key_path.display(), e))?;
        log::info!(
            "created new service identity for '{}': addr={}",
            name,
            identity.address_hash()
        );
        Ok(identity)
    }
}

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

fn default_interfaces() -> Vec<ReticulumInterface> {
    vec![ReticulumInterface {
        name: Some("Default Interface".to_string()),
        iface_type: "AutoInterface".to_string(),
        ..Default::default()
    }]
}

async fn spawn_interfaces(
    iface_mgr: &mut InterfaceManager,
    interfaces: &[ReticulumInterface],
    iface_mgr_arc: Arc<tokio::sync::Mutex<InterfaceManager>>,
) {
    let defaults = default_interfaces();
    let ifaces = if interfaces.is_empty() {
        log::info!("no interfaces configured, using AutoInterface default");
        defaults.as_slice()
    } else {
        interfaces
    };

    for iface in ifaces {
        let label = iface.name.as_deref().unwrap_or(&iface.iface_type);
        match iface.iface_type.as_str() {
            "AutoInterface" => {
                let port = iface.data_port.unwrap_or(49555);
                let bind = format!("0.0.0.0:{port}");
                let bcast = format!("255.255.255.255:{port}");
                let ui = UdpInterface::new(&bind, Some(&bcast));
                let addr = iface_mgr.spawn(ui, |ctx| UdpInterface::spawn(ctx));
                log::info!("spawned AutoInterface (UDP broadcast) port={} addr={}", port, addr);
            }
            "UDPInterface" => {
                let lip = iface.listen_ip.as_deref().unwrap_or("0.0.0.0");
                let lport = iface.listen_port.unwrap_or(4242);
                let bind = format!("{lip}:{lport}");
                let fwd = iface.forward_ip.as_ref().map(|ip| {
                    format!("{}:{}", ip, iface.forward_port.unwrap_or(lport))
                });
                let ui = UdpInterface::new(&bind, fwd.as_ref());
                let addr = iface_mgr.spawn(ui, |ctx| UdpInterface::spawn(ctx));
                log::info!("spawned UDPInterface '{}' bind={} forward={:?} addr={}", label, bind, fwd, addr);
            }
            "TCPServerInterface" => {
                let lip = iface.listen_ip.as_deref().unwrap_or("0.0.0.0");
                let lport = iface.listen_port.unwrap_or(7788);
                let bind = format!("{lip}:{lport}");
                let ts = TcpServer::new(&bind, iface_mgr_arc.clone());
                let addr = iface_mgr.spawn(ts, |ctx| TcpServer::spawn(ctx));
                log::info!("spawned TCPServerInterface '{}' bind={} addr={}", label, bind, addr);
            }
            "TCPClientInterface" => {
                let host = iface.target_host.as_deref().unwrap_or("");
                let port = iface.target_port.unwrap_or(7788);
                let target = format!("{host}:{port}");
                let tc = TcpClient::new(&target);
                let addr = iface_mgr.spawn(tc, |ctx| TcpClient::spawn(ctx));
                log::info!("spawned TCPClientInterface '{}' target={} addr={}", label, target, addr);
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

pub fn init_transport(cfg: &ReticulumConfig) -> Result<(), String> {
    if TRANSPORT.get().is_some() {
        log::debug!("Transport already initialized");
        return Ok(());
    }

    let cfg_dir = config_dir_path(cfg);
    if let Err(e) = std::fs::create_dir_all(&cfg_dir) {
        log::error!("failed to create config dir '{}': {}", cfg_dir, e);
        return Err(e.to_string());
    }
    log::info!("config dir: {}", cfg_dir);

    let identity = match resolve_identity(cfg, &cfg_dir) {
        Ok(id) => id,
        Err(e) => {
            log::error!("identity resolution failed: {}", e);
            return Err(e);
        }
    };
    log::info!("identity resolved: addr={}", identity.address_hash());

    let ratchet_store = Some(std::path::PathBuf::from(&cfg_dir).join("ratchet_store.db"));

    let mut tp_config = TransportConfig::new("sing-box-reticulum", &identity, true);
    tp_config.set_broadcast(true);
    tp_config.set_retransmit(true);
    if let Some(ref rstore) = ratchet_store {
        tp_config.set_ratchet_store_path(rstore.clone());
    }

    let interfaces = cfg.interfaces.clone();
    let transport = runtime::block_on(async move {
        let transport = Transport::new(tp_config);
        let iface_mgr = transport.iface_manager();
        let mut mgr = iface_mgr.lock().await;
        spawn_interfaces(&mut *mgr, &interfaces, iface_mgr.clone()).await;
        transport
    });
    log::info!("Transport initialized successfully");

    let _ = TRANSPORT.set(Arc::new(Mutex::new(transport)));
    Ok(())
}

// ---------------------------------------------------------------------------
// Service destination registration (server side)
// ---------------------------------------------------------------------------

/// Register a service destination. When a client link activates, calls
/// on_accept(listener_id, conn_id, peer_hash) instead of queuing.
pub async fn register_listener_destination(
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
        let hash = { let d = dest.lock().await; d.desc.address_hash };
        let events = tp.in_link_events();
        (hash, dest, events)
    };

    log::info!(
        "registered service destination: addr={} app={} aspect={}",
        address_hash, app_name, aspect
    );

    let listener_id = listener.id();
    let service_hash = address_hash;
    let store = crate::store::global_store();

    tokio::spawn(async move {
        loop {
            match link_events.recv().await {
                Ok(event) => {
                    if !matches!(event.event, LinkEvent::Activated) { continue; }

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
                            let peer_hash = event.address_hash.to_hex_string();
                            log::info!(
                                "service link activated: id={} peer={}",
                                event.id, peer_hash
                            );

                            let data_rx = {
                                let tp = transport.lock().await;
                                tp.received_data_events()
                            };

                            let conn = Connection::new_from_link(link.clone(), event.id, peer_hash.clone());
                            let conn_handle = store.insert_connection(conn).await;

                            spawn_link_data_reader(conn_handle, event.id, data_rx);
                            call_on_accept(listener_id, conn_handle, &peer_hash);
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::warn!("service link event channel closed");
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("service link event channel lagged by {}", n);
                    continue;
                }
            }
        }
    });

    Ok((address_hash, destination))
}

// ---------------------------------------------------------------------------
// Discovery destination (server side)
// ---------------------------------------------------------------------------

pub async fn start_service_announce_loop(
    service_dest: Arc<Mutex<SingleInputDestination>>,
    name: String,
    mut stop_rx: watch::Receiver<bool>,
) {
    loop {
        if *stop_rx.borrow() { break; }
        if let Some(tp) = get_transport() {
            tp.lock().await.send_announce(&service_dest, Some(name.as_bytes())).await;
            log::debug!("announced service dest for name='{}'", name);
        }
        tokio::select! {
            result = stop_rx.changed() => {
                if result.is_ok() && *stop_rx.borrow() { break; }
                tokio::time::sleep(ANNOUNCE_INTERVAL).await;
            }
            _ = tokio::time::sleep(ANNOUNCE_INTERVAL) => {}
        }
    }
    log::info!("service announce loop finished for name='{}'", name);
}

pub async fn register_discovery_destination(
    name: String,
    service_dest: Arc<Mutex<SingleInputDestination>>,
    service_hash: AddressHash,
) -> Result<AddressHash, &'static str> {
    let transport = get_transport().ok_or("Transport not initialized")?;

    let disc_identity = derive_discovery_identity(&name);
    let disc_dest_name = DestinationName::new("sing-box-reticulum", &format!("discovery.{}", name));

    let (disc_hash, mut link_events) = {
        let mut tp = transport.lock().await;
        let dest = tp.add_destination(disc_identity, disc_dest_name).await;
        let hash = { let d = dest.lock().await; d.desc.address_hash };
        let events = tp.in_link_events();
        (hash, events)
    };

    log::info!(
        "registered discovery destination: addr={} name='{}'",
        disc_hash, name
    );

    tokio::spawn(async move {
        let mut stop_tx: Option<watch::Sender<bool>> = None;
        loop {
            match link_events.recv().await {
                Ok(event) => {
                    if !matches!(event.event, LinkEvent::Activated) { continue; }

                    let tp_arc = match get_transport() {
                        Some(t) => t,
                        None => break,
                    };
                    let link = {
                        let tp = tp_arc.lock().await;
                        tp.find_in_link(&event.id).await
                    };
                    let link_dest = match link {
                        Some(l) => { let guard = l.lock().await; guard.destination().address_hash }
                        None => continue,
                    };

                    if link_dest == disc_hash {
                        log::info!("discovery knock received for name='{}'", name);
                        if let Some(tx) = stop_tx.take() { let _ = tx.send(true); }
                        let (tx, rx) = watch::channel(false);
                        stop_tx = Some(tx);
                        let dest_clone = service_dest.clone();
                        let name_clone = name.clone();
                        tokio::spawn(async move {
                            start_service_announce_loop(dest_clone, name_clone, rx).await;
                        });
                    } else if link_dest == service_hash {
                        log::info!(
                            "service link established, stopping announce for name='{}'",
                            name
                        );
                        if let Some(tx) = stop_tx.take() { let _ = tx.send(true); }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::warn!("discovery link event channel closed");
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("discovery link event lagged by {}", n);
                    continue;
                }
            }
        }
    });

    Ok(disc_hash)
}

// ---------------------------------------------------------------------------
// Client-side discovery
// ---------------------------------------------------------------------------

pub async fn wait_for_service_announce(name: &str, timeout: Duration) -> Option<String> {
    let transport = get_transport()?;
    let mut announces = {
        let tp = transport.lock().await;
        tp.recv_announces().await
    };
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { return None; }
        tokio::select! {
            _ = tokio::time::sleep(remaining) => return None,
            result = announces.recv() => {
                match result {
                    Ok(event) if event.app_data.as_slice() == name.as_bytes() => {
                        let dest = event.destination.lock().await;
                        let hash = dest.desc.address_hash.to_hex_string();
                        log::info!("received service announce for '{}': hash={}", name, hash);
                        return Some(hash);
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        }
    }
}

pub async fn dial_discovery_and_wait(name: &str) -> Result<(), &'static str> {
    let transport = get_transport().ok_or("Transport not initialized")?;

    let disc_private = derive_discovery_identity(name);
    let disc_public: Identity = *disc_private.as_identity();
    let disc_dest_name = DestinationName::new("sing-box-reticulum", &format!("discovery.{}", name));
    let disc_hash_str = discovery_hash_for_name(name);
    let disc_hash =
        AddressHash::new_from_hex_string(&disc_hash_str).map_err(|_| "invalid discovery hash")?;

    let desc = DestinationDesc { identity: disc_public, address_hash: disc_hash, name: disc_dest_name };

    log::info!("sending discovery knock for name='{}'", name);

    let link = {
        let tp = transport.lock().await;
        tp.link(desc).await
    };

    let start = tokio::time::Instant::now();
    let knock_timeout = Duration::from_secs(5);
    loop {
        if start.elapsed() >= knock_timeout {
            log::warn!("discovery knock timed out for name='{}'", name);
            return Ok(());
        }
        let status = { link.lock().await.status() };
        match status {
            LinkStatus::Active | LinkStatus::Closed | LinkStatus::Stale => {
                log::info!("discovery knock completed (status={:?}) for name='{}'", status, name);
                return Ok(());
            }
            _ => {}
        }
        tokio::time::sleep(DIAL_POLL_INTERVAL).await;
    }
}

// ---------------------------------------------------------------------------
// Data reader (fires on_data and on_close callbacks)
// ---------------------------------------------------------------------------

/// Spawn a background task that forwards inbound link data via the on_data
/// callback, and fires on_close when the channel closes.
pub fn spawn_link_data_reader(
    conn_handle: u64,
    link_id: AddressHash,
    mut data_rx: broadcast::Receiver<ReceivedData>,
) {
    tokio::spawn(async move {
        loop {
            match data_rx.recv().await {
                Ok(data) if data.destination == link_id => {
                    call_on_data(conn_handle, data.data.as_slice());
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    log::debug!("data channel closed for conn {}", conn_handle);
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
        call_on_close(conn_handle);
    });
}

// ---------------------------------------------------------------------------
// Outbound dial
// ---------------------------------------------------------------------------

fn parse_dest_hash(hex_str: &str) -> Result<AddressHash, &'static str> {
    let clean = hex_str
        .strip_prefix("rln://")
        .or_else(|| hex_str.strip_prefix("0x"))
        .unwrap_or(hex_str);
    AddressHash::new_from_hex_string(clean).map_err(|_| "invalid destination hash hex string")
}

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
    let desc = DestinationDesc { identity, address_hash, name };

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
                log::info!("link {} active (poll), dial successful", link_id);
                return Ok((link_clone, link_id));
            }
            LinkStatus::Closed | LinkStatus::Stale => {
                log::error!("link {} failed with status {:?}", link_id, link_status);
                return Err("link failed before becoming active");
            }
            _ => {}
        }

        tokio::time::sleep(DIAL_POLL_INTERVAL).await;
        match link_events.try_recv() {
            Ok(event) if event.id == link_id && matches!(event.event, LinkEvent::Activated) => {
                let cur_status = { link_clone.lock().await.status() };
                log::info!("link {} activated (event) cur_status={:?}, dial successful", link_id, cur_status);
                return Ok((link_clone, link_id));
            }
            Ok(_) => {}
            Err(broadcast::error::TryRecvError::Empty) => {}
            Err(broadcast::error::TryRecvError::Closed) => {
                log::error!("link event channel closed while dialing");
                return Err("link event channel closed");
            }
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
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
        assert_eq!(result.unwrap().to_hex_string(), "aabbccdd00112233445566778899aabb");
    }

    #[test]
    fn test_parse_dest_hash_invalid_hex() {
        let result = parse_dest_hash("not-hex-string");
        assert!(result.is_err());
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
    fn test_discovery_hash_deterministic() {
        let h1 = discovery_hash_for_name("e2e-sum-server");
        let h2 = discovery_hash_for_name("e2e-sum-server");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 32);

        let h3 = discovery_hash_for_name("other-server");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_identity_persistence() {
        let dir = std::env::temp_dir().join(format!("test-identity-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg_dir = dir.to_str().unwrap();

        let id1 = load_or_create_service_identity(cfg_dir, "test-service").unwrap();
        let id2 = load_or_create_service_identity(cfg_dir, "test-service").unwrap();
        assert_eq!(id1.to_hex_string(), id2.to_hex_string());

        let id3 = load_or_create_service_identity(cfg_dir, "other-service").unwrap();
        assert_ne!(id1.to_hex_string(), id3.to_hex_string());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_wait_for_announce_no_transport() {
        let result = wait_for_service_announce("nonexistent", Duration::from_millis(50)).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_dial_and_wait_no_transport() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(dial_and_wait("rln://aabbccdd00112233445566778899aabb"));
        let err = result.err().expect("should be Err");
        assert_eq!(err, "Transport not initialized");
    }
}
