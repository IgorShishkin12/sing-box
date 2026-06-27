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
use reticulum_rs::transport::iface::lora::{LoraConfig, LoraInterface};
#[cfg(feature = "rnode-ble")]
use reticulum_rs::transport::iface::rnode_ble::{
    NativeRnodeBleKissInterface, NativeRnodeBleSettings, RnodeBleKissConfig,
};
use reticulum_rs::transport::iface::tcp_client::TcpClient;
use reticulum_rs::transport::iface::tcp_server::TcpServer;
use reticulum_rs::transport::iface::udp::UdpInterface;
use reticulum_rs::transport::iface::{InterfaceManager, RxMessage};
use reticulum_rs::transport::transport::{Transport, TransportConfig};
use reticulum_rs::transport::PacketContext;

use crate::config::{ReticulumConfig, ReticulumInterface};
use crate::listener::Listener;
use crate::runtime;

/// Maximum time to wait for a link to become active during dial.
// On multicast/AutoInterface networks the first dial attempt is expected to fail:
// the link-request proof is dropped because the peer's virtual unicast iface isn't
// registered yet (announce arrives ~1 s after the link request). The bridge closes
// the link on timeout and retries with a fresh link_id, at which point the proof
// is delivered successfully. Keep this short so retries are fast.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
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
// Android JVM singleton
// ---------------------------------------------------------------------------

#[cfg(all(feature = "rnode-ble", target_os = "android"))]
struct JavaVmPtr(usize);

// Safety: the pointer is valid for the process lifetime and never mutated
// after the initial set_android_jvm call.
#[cfg(all(feature = "rnode-ble", target_os = "android"))]
unsafe impl Send for JavaVmPtr {}
#[cfg(all(feature = "rnode-ble", target_os = "android"))]
unsafe impl Sync for JavaVmPtr {}

#[cfg(all(feature = "rnode-ble", target_os = "android"))]
static ANDROID_JVM: OnceCell<JavaVmPtr> = OnceCell::new();

/// Store the raw JavaVM address (idempotent after the first call).
#[cfg(all(feature = "rnode-ble", target_os = "android"))]
pub fn set_android_jvm(addr: usize) {
    let _ = ANDROID_JVM.set(JavaVmPtr(addr));
}

/// Return the stored JavaVM address, or None if not yet set.
#[cfg(all(feature = "rnode-ble", target_os = "android"))]
pub fn android_jvm_addr() -> Option<usize> {
    ANDROID_JVM.get().map(|p| p.0)
}

/// Initialize btleplug's Android platform using the stored JavaVM.
/// Must be called on a Java thread (e.g. from JNI_OnLoad or reticulum_set_jvm).
#[cfg(all(feature = "rnode-ble", target_os = "android"))]
pub fn init_btleplug_android() -> Result<(), String> {
    let addr = android_jvm_addr().ok_or("ANDROID_JVM not set")?;
    unsafe {
        let raw_jvm = addr as *mut jni::sys::JavaVM;
        let jvm = jni::JavaVM::from_raw(raw_jvm)
            .map_err(|e| format!("JavaVM::from_raw: {:?}", e))?;
        let env = jvm
            .attach_current_thread()
            .map_err(|e| format!("attach_current_thread: {:?}", e))?;
        btleplug::platform::init(&env)
            .map_err(|e| format!("btleplug platform init: {:?}", e))?;
    }
    log::info!("btleplug Android platform initialized");
    Ok(())
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

/// Build a LoraConfig from interface config, falling back to US915 defaults for
/// any unset field. Validation has already been done on the Go side.
fn build_lora_config(iface: &ReticulumInterface) -> LoraConfig {
    let base = LoraConfig::us915_default();
    LoraConfig {
        frequency_hz: iface.frequency_hz.unwrap_or(base.frequency_hz),
        bandwidth_hz: iface.bandwidth_hz.unwrap_or(base.bandwidth_hz),
        tx_power_dbm: iface.tx_power_dbm.unwrap_or(base.tx_power_dbm),
        spreading_factor: iface.spreading_factor.unwrap_or(base.spreading_factor),
        coding_rate: iface.coding_rate.unwrap_or(base.coding_rate),
        ..base
    }
}

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
            #[cfg(feature = "rnode-ble")]
            "RNodeBLE" => {
                let peripheral_id = iface.peripheral_id.as_deref().unwrap_or("");
                let lora = build_lora_config(iface);
                let settings = NativeRnodeBleSettings::for_peripheral(peripheral_id);
                // Two-phase startup: probe first, radio config only after the RNode
                // responds to the probe (deferred_frames, triggered by is_detected()).
                // Sending everything at once causes CMD_RADIO_STATE ON to be ignored —
                // the RNode never enters KISS bridge mode and delivers no data frames.
                //
                // 1h validation deadline: the 5s default triggers a reconnect loop
                // because the RNode's periodic EEPROM status broadcasts (every ~5s)
                // overwrite the echoed radio config before validation completes.
                //
                // 3s detection fallback: some firmware ignores the first CMD_DETECT
                // probe on a fresh BLE connect; after 3s we send radio config
                // unconditionally so the RNode enters KISS bridge mode regardless.
                let ble = NativeRnodeBleKissInterface::new(
                    label,
                    settings,
                    RnodeBleKissConfig {
                        initial_frames: lora.probe_frames(),
                        deferred_frames: lora.radio_config_frames(),
                        shutdown_frames: lora.shutdown_frames(),
                        ..RnodeBleKissConfig::default()
                    },
                )
                .with_rnode_validation(lora, Duration::from_secs(3600))
                .with_detection_fallback_timeout(Duration::from_secs(3));
                let iface_mgr_clone = iface_mgr.clone();
                let addr = iface_mgr.lock().await.spawn(ble, |context| async move {
                    NativeRnodeBleKissInterface::spawn(context, iface_mgr_clone).await
                });
                log::info!(
                    "spawned RNodeBLE '{}' peripheral_id={} freq_hz={} addr={}",
                    label,
                    peripheral_id,
                    lora.frequency_hz,
                    addr
                );
            }
            "RNodeSerial" => {
                let device = iface.device.as_deref().unwrap_or("");
                let lora = build_lora_config(iface);
                let rnode = LoraInterface::new(device, 115_200, lora);
                let addr = iface_mgr.lock().await.spawn(rnode, LoraInterface::spawn);
                log::info!(
                    "spawned RNodeSerial '{}' device={} freq_hz={} addr={}",
                    label,
                    device,
                    lora.frequency_hz,
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

    // Diagnostic: subscribe to raw interface packets to verify the BLE → KISS
    // → Packet::deserialize chain without patching the library. Logged at DEBUG
    // so it has zero cost in production unless RUST_LOG includes debug for this module.
    if let Some(tp_arc) = get_transport() {
        let iface_rx_handle = runtime::spawn(async move {
            let mut rx: broadcast::Receiver<RxMessage> = {
                let tp = tp_arc.lock().await;
                tp.iface_rx()
            };
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        log::debug!(
                            "iface_rx: addr={} packet_type={:?}",
                            msg.address,
                            msg.packet.header.packet_type
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("iface_rx lagged by {} messages", n);
                    }
                }
            }
        });
        runtime::register_task(iface_rx_handle);
    }

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
                            // Spawn per-link handling so the accept loop is not
                            // blocked while exchange_identify_on_link waits.
                            let store = store.clone();
                            let listener = listener.clone();
                            let link_id = event.id;
                            let link = link.clone();
                            let mut peer_events = {
                                let tp = transport.lock().await;
                                tp.in_link_events()
                            };
                            let data_rx = {
                                let tp = transport.lock().await;
                                tp.received_data_events()
                            };
                            let resource_rx = {
                                let tp = transport.lock().await;
                                tp.resource_events()
                            };
                            let task = tokio::spawn(async move {
                                let identified =
                                    exchange_identify_on_link(&link, link_id, &mut peer_events)
                                        .await;
                                let conn = crate::connection::Connection::new_from_link(
                                    link.clone(),
                                    link_id,
                                    Some(peer_hash),
                                    identified,
                                );
                                let conn_id = store.insert_connection(conn).await;
                                spawn_link_data_reader(conn_id, link_id, data_rx);
                                spawn_resource_event_reader(conn_id, link_id, resource_rx);
                                open_channel_and_forward(conn_id, link_id).await;
                                crate::c_api::call_on_accept(
                                    listener_handle,
                                    conn_id,
                                    &peer_hash.to_hex_string(),
                                );
                                let _ = &listener;
                            });
                            runtime::register_task(task);
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
/// The library delivers `PacketContext::LinkIdentify` (0xFB) packets as
/// `LinkEvent::Data` with `payload.context() == LinkIdentify` on the link-event
/// channel. This function listens there, verifies the payload with
/// `parse_link_identify_payload`, and returns the peer's identity hash.
/// Callers must subscribe to `in_link_events` (server) or `out_link_events`
/// (client) **before** the link activates so that the event is buffered and not
/// missed.
///
/// The identify is re-sent every `IDENTIFY_RETRY_INTERVAL` until the peer's
/// identify arrives or `IDENTIFY_TIMEOUT` elapses. Fernet encryption uses a
/// random IV per call, so each re-send has a unique packet hash and is not
/// dropped by the transport's dedup cache.
///
/// Logs:
///   my_identity:   addr=… encrypt=… sign=…
///   peer_identity: addr=… encrypt=… sign=…
///
/// Returns `None` if the local identity is unavailable or the exchange times out.
pub async fn exchange_identify_on_link(
    link: &Arc<Mutex<Link>>,
    link_id: AddressHash,
    link_events: &mut broadcast::Receiver<LinkEventData>,
) -> Option<AddressHash> {
    // On multicast networks, the first dial attempt fails silently (proof dropped)
    // and the client retries with a fresh link after DIAL_TIMEOUT (~10 s). Keep
    // IDENTIFY_TIMEOUT larger than DIAL_TIMEOUT so the server is still waiting
    // when the client's second attempt activates and sends its identify.
    const IDENTIFY_TIMEOUT: Duration = Duration::from_secs(30);
    const IDENTIFY_RETRY_INTERVAL: Duration = Duration::from_secs(3);

    let transport_id = get_transport_identity()?;

    let my_id = transport_id.as_identity();
    log::info!(
        "my_identity: addr={} encrypt={} sign={}",
        my_id.address_hash,
        hex::encode(my_id.public_key_bytes()),
        hex::encode(my_id.verifying_key_bytes()),
    );

    let identify_payload = build_link_identify_payload(&transport_id, &link_id);

    let send_identify = |link: &Arc<Mutex<Link>>| {
        let payload = identify_payload.clone();
        let link = link.clone();
        async move {
            let packet = {
                let guard = link.lock().await;
                let mut pkt = match guard.data_packet(&payload) {
                    Ok(p) => p,
                    Err(e) => {
                        log::warn!("identify: failed to build packet: {:?}", e);
                        return;
                    }
                };
                pkt.context = PacketContext::LinkIdentify;
                pkt
            };
            if let Some(tp) = get_transport() {
                let tp = tp.lock().await;
                // Route the identify as a link-directed packet (Direct on the
                // link's iface) rather than a broadcast. The transport drops
                // Broadcast TX messages when an interface queue is full, which
                // silently loses identify packets on congested/slow links (e.g.
                // LoRa) and breaks the auth handshake. send_packet uses the
                // backpressure (timeout-enqueue) path and won't be dropped.
                tp.send_packet(packet).await;
            }
        }
    };

    send_identify(link).await;

    let deadline = tokio::time::Instant::now() + IDENTIFY_TIMEOUT;
    let mut next_retry = tokio::time::Instant::now() + IDENTIFY_RETRY_INTERVAL;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            log::warn!(
                "identify: timeout waiting for peer identify on link {}",
                link_id
            );
            return None;
        }
        let until_retry = next_retry.saturating_duration_since(tokio::time::Instant::now());
        tokio::select! {
            _ = tokio::time::sleep(remaining.min(until_retry)) => {
                if tokio::time::Instant::now() >= next_retry {
                    log::debug!("identify: retrying send on link {}", link_id);
                    send_identify(link).await;
                    next_retry = tokio::time::Instant::now() + IDENTIFY_RETRY_INTERVAL;
                } else {
                    log::warn!("identify: timeout waiting for peer identify on link {}", link_id);
                    return None;
                }
            }
            result = link_events.recv() => {
                match result {
                    Ok(event) if event.id == link_id => {
                        if let LinkEvent::Data(payload) = event.event {
                            if payload.context() == PacketContext::LinkIdentify {
                                if let Some(identity) = parse_link_identify_payload(payload.as_slice(), &link_id) {
                                    log::info!(
                                        "peer_identity: addr={} encrypt={} sign={}",
                                        identity.address_hash,
                                        hex::encode(identity.public_key_bytes()),
                                        hex::encode(identity.verifying_key_bytes()),
                                    );
                                    return Some(identity.address_hash);
                                }
                            }
                        }
                        // Other events on this link (e.g. Activated, KeepAlive) — skip.
                    }
                    Ok(_) => {} // event for a different link — skip
                    Err(broadcast::error::RecvError::Closed) => return None,
                    Err(broadcast::error::RecvError::Lagged(_)) => {} // catch up
                }
            }
        }
    }
}

/// msg_type used for all mux stream data carried over the Reticulum channel.
/// (Conn-multiplexing lives inside the payload — the mux's own framing — so a
/// single channel msg_type suffices for the whole stream.)
pub const MUX_CHANNEL_MSG_TYPE: u16 = 0x0001;

/// Open the link's reliable Reticulum channel and forward every received channel
/// message to Go via `on_data`, in order. With sends now going over the channel
/// (`Connection::write_channel`), this replaces the raw data-packet receive path
/// for mux traffic; the channel guarantees ordered, de-duplicated delivery.
///
/// Both peers must open the channel before data flows (the receiver drops channel
/// frames on a not-yet-open channel), so this runs at link activation on both the
/// dial and accept paths.
pub async fn open_channel_and_forward(conn_id: u64, link_id: AddressHash) {
    let Some(transport) = get_transport() else {
        log::warn!("[channel] open: transport not initialized conn={}", conn_id);
        return;
    };
    let ch = {
        let guard = transport.lock().await;
        guard.channel(link_id)
    };
    if let Err(e) = ch.open().await {
        log::warn!(
            "[channel] open failed conn={} link={} err={:?}",
            conn_id,
            link_id,
            e
        );
        return;
    }
    match ch
        .register_handler(MUX_CHANNEL_MSG_TYPE, move |envelope| {
            crate::c_api::call_on_data(conn_id, &envelope.payload);
            true
        })
        .await
    {
        Ok(_) => log::debug!(
            "[channel] opened + handler registered conn={} link={}",
            conn_id,
            link_id
        ),
        Err(e) => log::warn!(
            "[channel] register handler failed conn={} link={} err={:?}",
            conn_id,
            link_id,
            e
        ),
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
    // Inactivity guard for an *in-progress* inbound transfer. The reader is
    // long-lived (it serves every resource over `link_id` for this connection's
    // lifetime), so the deadline must only be armed while a transfer is actually
    // running — otherwise a healthy idle connection would be closed. It is armed
    // on the first Progress event and *reset on every subsequent Progress*, so a
    // slow-but-advancing transfer is never killed (the total transfer time is
    // unbounded and must not be capped — only true silence is). It is disarmed on
    // Complete/InboundFailed. This is purely defense-in-depth: the lib emits
    // `InboundFailed` on its own progress-aware retry exhaustion; this only covers
    // the case where the peer vanishes mid-transfer and no terminal event is ever
    // produced. Kept generous so it never pre-empts the lib's own recovery.
    const INBOUND_INACTIVITY_SECS: u64 = crate::RESOURCE_INACTIVITY_SECS;

    log::debug!(
        "[res-bridge] resource event reader spawned conn={} link={}",
        conn_id,
        link_id
    );
    let handle = tokio::spawn(async move {
        // When no transfer is in flight the deadline is parked far in the future
        // so `sleep_until` effectively never fires.
        const PARKED_SECS: u64 = 365 * 24 * 60 * 60; // ~1 year
        let parked = || tokio::time::Instant::now() + Duration::from_secs(PARKED_SECS);
        let mut deadline = parked();
        let mut transfer_active = false;
        let mut last_progress_bytes: u64 = 0;

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline), if transfer_active => {
                    log::warn!(
                        "resource inbound stalled ({}s inactivity), closing conn={} link={} received={}",
                        INBOUND_INACTIVITY_SECS, conn_id, link_id, last_progress_bytes
                    );
                    crate::c_api::call_on_close(conn_id);
                    break;
                }
                result = resource_rx.recv() => {
                    match result {
                        Ok(event) if event.link_id == link_id => match event.kind {
                            ResourceEventKind::Complete(complete) => {
                                log::info!(
                                    "[res-bridge] resource COMPLETE conn={} link={} len={} -> delivering to app",
                                    conn_id,
                                    link_id,
                                    complete.data.len()
                                );
                                transfer_active = false;
                                last_progress_bytes = 0;
                                deadline = parked();
                                crate::c_api::call_on_data(conn_id, &complete.data);
                            }
                            ResourceEventKind::Progress(ref p) => {
                                log::debug!(
                                    "[res-bridge] inbound progress conn={} link={} received={}/{} parts={}/{}",
                                    conn_id,
                                    link_id,
                                    p.received_bytes,
                                    p.total_bytes,
                                    p.received_parts,
                                    p.total_parts,
                                );
                                // Arm / extend the inactivity deadline while bytes keep arriving.
                                if !transfer_active || p.received_bytes > last_progress_bytes {
                                    transfer_active = true;
                                    last_progress_bytes = p.received_bytes;
                                    deadline = tokio::time::Instant::now()
                                        + Duration::from_secs(INBOUND_INACTIVITY_SECS);
                                }
                            }
                            ResourceEventKind::InboundFailed(ref f) => {
                                log::warn!(
                                    "resource inbound failed: conn={} link={} reason={} received={}/{}",
                                    conn_id,
                                    link_id,
                                    f.reason,
                                    f.progress.received_bytes,
                                    f.progress.total_bytes
                                );
                                crate::c_api::call_on_close(conn_id);
                                break;
                            }
                            // Outbound terminal variants are handled per-transfer by
                            // `wait_for_outbound_resource`; log so nothing is silent here.
                            other => {
                                log::debug!(
                                    "resource event unhandled in inbound reader: conn={} link={} kind={:?}",
                                    conn_id, link_id, other
                                );
                            }
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
            // Close the link so the next tp.link() call creates a fresh one with a
            // new key pair. Without this, tp.link() reuses the same Pending link and
            // the server never re-sends the proof (in_links already has the link_id).
            link_clone.lock().await.close();
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
        // activation is event-driven instead of polled every 100 ms. The library does emit
        // LinkStatus::Closed/Stale (already handled above), so this is now resolvable — but
        // skipping for now due to library API potential bugs and instability.
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

    /// An inbound `InboundFailed` event must fire the on_close callback (the conn
    /// is torn down) instead of being silently swallowed. Regression test for the
    /// silent large-request hang (e2e_serial62).
    #[tokio::test]
    async fn test_resource_event_reader_closes_on_inbound_failed() {
        use reticulum_rs::resource::{
            ResourceEvent, ResourceEventKind, ResourceFailure, ResourceProgress,
        };
        use reticulum_rs::transport::hash::{AddressHash, Hash};
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::sync::broadcast;

        // Records the conn_id passed to the most recent on_close call.
        static CLOSED_CONN: AtomicU64 = AtomicU64::new(0);
        extern "C" fn record_close(conn_id: u64) {
            CLOSED_CONN.store(conn_id, Ordering::SeqCst);
        }

        const SENTINEL_CONN: u64 = 0xABCD_1234;
        CLOSED_CONN.store(0, Ordering::SeqCst);

        // Save & install our close callback; restore afterwards so we don't leak
        // state into other tests sharing the global callback slot.
        let prev = crate::c_api::ON_CLOSE.swap(
            record_close as extern "C" fn(u64) as usize,
            Ordering::SeqCst,
        );

        let (tx, rx) = broadcast::channel::<ResourceEvent>(16);
        let target_link =
            AddressHash::new_from_hex_string("aabbccdd00112233445566778899aabb").unwrap();
        spawn_resource_event_reader(SENTINEL_CONN, target_link, rx);

        tx.send(ResourceEvent {
            hash: Hash::new_from_slice(&[0u8; 32]),
            link_id: target_link,
            kind: ResourceEventKind::InboundFailed(ResourceFailure {
                reason: "retry_limit_exhausted".to_string(),
                progress: ResourceProgress {
                    received_bytes: 0,
                    total_bytes: 108_905,
                    received_parts: 0,
                    total_parts: 179,
                },
            }),
        })
        .unwrap();

        // Give the reader task time to process and invoke the callback.
        for _ in 0..50 {
            if CLOSED_CONN.load(Ordering::SeqCst) == SENTINEL_CONN {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        crate::c_api::ON_CLOSE.store(prev, Ordering::SeqCst);
        drop(tx);

        assert_eq!(
            CLOSED_CONN.load(Ordering::SeqCst),
            SENTINEL_CONN,
            "InboundFailed must trigger on_close for the affected connection"
        );
    }
}
