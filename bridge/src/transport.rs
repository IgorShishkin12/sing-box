//! Global Transport singleton backed by `reticulum-rs` real networking.
//!
//! This module is only compiled when the `real-reticulum` feature is active.
//! It manages a lazily-initialized `reticulum_rs::transport::Transport` instance
//! that owns the Reticulum runtime, interface manager, link tables, and destination
//! registration.
//!
//! It also provides helpers for registering `SingleInputDestination`s and subscribing
//! to link events, used by the listener accept loop.
//!
//! IMPORTANT: `reticulum_rs` re-exports `rns_core` types at the top level
//! (e.g. `reticulum_rs::destination`, `reticulum_rs::identity`, `reticulum_rs::hash`)
//! and `rns_transport` types under `reticulum_rs::transport::*`. These are *distinct
//! types* even when they have the same name. The `Transport` API uses `rns_transport`
//! types, so all imports here must reference `reticulum_rs::transport::*` sub-modules.

#![cfg(feature = "real-reticulum")]

use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::Mutex;

use rand_core::OsRng;
use reticulum_rs::transport::destination::link::{LinkEvent, LinkEventData};
use reticulum_rs::transport::destination::{DestinationName, SingleInputDestination};
use reticulum_rs::transport::hash::AddressHash;
use reticulum_rs::transport::identity::PrivateIdentity;
use reticulum_rs::transport::iface::udp::UdpInterface;
use reticulum_rs::transport::iface::InterfaceManager;
use reticulum_rs::transport::transport::{Transport, TransportConfig};

use crate::config::{ReticulumConfig, ReticulumInterface};
use crate::connection::Connection;
use crate::listener::Listener;
use crate::runtime;

/// Global Transport singleton.
static TRANSPORT: OnceCell<Arc<Mutex<Transport>>> = OnceCell::new();

/// Get a reference to the global Transport, if initialized.
pub fn get_transport() -> Option<&'static Arc<Mutex<Transport>>> {
    TRANSPORT.get()
}

/// Resolve the identity from config, following the priority:
/// 1. `identity_key` → `PrivateIdentity::new_from_hex_string`
/// 2. `identity_name` → `PrivateIdentity::new_from_rand` (non-deterministic random)
/// 3. Otherwise → error
fn resolve_identity(cfg: &ReticulumConfig) -> Result<PrivateIdentity, &'static str> {
    if let Some(ref key) = cfg.identity_key {
        PrivateIdentity::new_from_hex_string(key).map_err(|_| "invalid identity_key hex string")
    } else if let Some(ref _name) = cfg.identity_name {
        // Generate a random identity (non-deterministic). The name is used
        // only as a human-readable tag, not as a key seed.
        Ok(PrivateIdentity::new_from_rand(OsRng))
    } else {
        Err("config must contain either 'identity_key' (128-char hex) or 'identity_name'")
    }
}

/// Determine the Reticulum config directory path.
/// Returns `<config_dir>` if set, otherwise `$HOME/.reticulum`.
fn config_dir_path(cfg: &ReticulumConfig) -> String {
    if let Some(ref dir) = cfg.config_dir {
        dir.clone()
    } else if let Ok(home) = std::env::var("HOME") {
        format!("{}/.reticulum", home)
    } else {
        // Last resort fallback
        "/etc/reticulum".to_string()
    }
}

/// Parse an interface spec string and spawn the corresponding interface on the
/// transport's InterfaceManager.
///
/// Supported formats:
/// - `udp <bind_host> <bind_port>`
/// - `udp <bind_host> <bind_port> <peer_host> <peer_port>`
async fn spawn_interfaces(iface_mgr: &mut InterfaceManager, interfaces: &[ReticulumInterface]) {
    for iface_spec in interfaces {
        let parts: Vec<&str> = iface_spec.r#type.split_whitespace().collect();
        if parts.is_empty() {
            eprintln!("[bridge-tp] empty interface spec, skipping");
            continue;
        }
        match parts[0] {
            "udp" => {
                if parts.len() < 3 {
                    eprintln!("[bridge-tp] udp interface requires at least <bind_host> <bind_port>");
                    continue;
                }
                let bind_addr = format!("{}:{}", parts[1], parts[2]);
                let forward_addr = if parts.len() >= 5 {
                    Some(format!("{}:{}", parts[3], parts[4]))
                } else {
                    None
                };
                let udp_iface = UdpInterface::new(&bind_addr, forward_addr.as_ref());
                let addr = iface_mgr.spawn(udp_iface, |ctx| UdpInterface::spawn(ctx));
                eprintln!(
                    "[bridge-tp] spawned UDP interface bind={} forward={:?} addr={}",
                    bind_addr, forward_addr, addr
                );
            }
            other => {
                eprintln!("[bridge-tp] unknown interface type '{}', skipping", other);
            }
        }
    }
}

/// Initialize the global Transport singleton with the given config.
/// Must be called once during `reticulum_init`. Returns 0 on success, -1 on error.
pub fn init_transport(cfg: &ReticulumConfig) -> i32 {
    if TRANSPORT.get().is_some() {
        eprintln!("[bridge-tp] Transport already initialized");
        return 0;
    }

    // 1. Resolve identity
    let identity = match resolve_identity(cfg) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("[bridge-tp] identity resolution failed: {}", e);
            return -1;
        }
    };

    eprintln!(
        "[bridge-tp] identity resolved: addr={}",
        identity.address_hash()
    );

    // 2. Determine config directory and create it if needed
    let cfg_dir = config_dir_path(cfg);
    if let Err(e) = std::fs::create_dir_all(&cfg_dir) {
        eprintln!("[bridge-tp] failed to create config dir '{}': {}", cfg_dir, e);
        return -1;
    }
    eprintln!("[bridge-tp] config dir: {}", cfg_dir);

    // 3. Build ratchet store path
    let ratchet_store = Some(std::path::PathBuf::from(&cfg_dir).join("ratchet_store.db"));

    // 4. Build TransportConfig
    let mut tp_config = TransportConfig::new("sing-box-reticulum", &identity, true);
    tp_config.set_broadcast(true);
    tp_config.set_retransmit(true);
    if let Some(ref rstore) = ratchet_store {
        tp_config.set_ratchet_store_path(rstore.clone());
    }

    // 5. Create Transport (this spawns background tasks internally)
    let transport = Transport::new(tp_config);

    // 6. Spawn interfaces from config (async, run via block_on)
    let iface_mgr = transport.iface_manager();
    let interfaces = cfg.interfaces.clone();
    runtime::block_on(async move {
        let mut mgr = iface_mgr.lock().await;
        spawn_interfaces(&mut *mgr, &interfaces).await;
    });

    eprintln!("[bridge-tp] Transport initialized successfully");

    // 7. Store globally
    let _ = TRANSPORT.set(Arc::new(Mutex::new(transport)));
    0
}

/// Register a `SingleInputDestination` with the global Transport and spawn a
/// background task that subscribes to `LinkEvent::Activated` events, creating
/// a `Connection` wrapping the established `Link` and pushing it into the
/// listener's accept queue.
///
/// Returns the `AddressHash` (from `rns_core`) of the registered destination.
/// The caller should store this in the Listener for later reference.
pub async fn register_listener_destination(
    listener: Arc<Listener>,
    identity: PrivateIdentity,
    app_name: String,
    aspect: String,
) -> Result<AddressHash, &'static str> {
    let transport = get_transport()
        .ok_or("Transport not initialized")?;

    let mut tp = transport.lock().await;

    // Create the destination name and register it with the transport
    let name = DestinationName::new(&app_name, &aspect);
    let destination = tp.add_destination(identity, name).await;
    let address_hash = {
        let dest = destination.lock().await;
        dest.desc.address_hash
    };

    eprintln!(
        "[bridge-tp] registered listener destination: addr={} app={} aspect={}",
        address_hash, app_name, aspect
    );

    // Subscribe to incoming link events
    let mut link_events = tp.in_link_events();

    // Spawn a background task that monitors link events and pushes
    // established links into the listener's accept queue.
    let listener_clone = listener.clone();
    tokio::spawn(async move {
        loop {
            match link_events.recv().await {
                Ok(event) => {
                    match event.event {
                        LinkEvent::Activated => {
                            eprintln!(
                                "[bridge-tp] link activated: id={} peer={}",
                                event.id, event.address_hash
                            );

                            // Find the link in the transport's in_links
                            let transport = match get_transport() {
                                Some(t) => t,
                                None => {
                                    eprintln!("[bridge-tp] transport gone, stopping link listener");
                                    break;
                                }
                            };

                            let link = {
                                let tp = transport.lock().await;
                                tp.find_in_link(&event.id).await
                            };

                            if let Some(link) = link {
                                // Create a Connection wrapping this link
                                let conn = Connection::new_from_link(link.clone(), event.id);

                                // Subscribe to data events on this link
                                let mut data_events = {
                                    let tp = transport.lock().await;
                                    tp.received_data_events()
                                };
                                let conn_clone = conn.clone();
                                tokio::spawn(async move {
                                    loop {
                                        match data_events.recv().await {
                                            Ok(data) => {
                                                if data.destination == event.id {
                                                    conn_clone.push_read_data(data.data.as_slice()).await;
                                                }
                                            }
                                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                        }
                                    }
                                });

                                // Push the connection into the listener's accept queue
                                listener_clone.push_connection(conn).await;
                                eprintln!(
                                    "[bridge-tp] pushed link connection to listener accept queue"
                                );
                            } else {
                                eprintln!(
                                    "[bridge-tp] link {} not found in transport in_links",
                                    event.id
                                );
                            }
                        }
                        LinkEvent::Data(_) => {
                            // Data events are handled by the received_data_events subscriber
                        }
                        LinkEvent::Closed => {
                            eprintln!(
                                "[bridge-tp] link closed: id={} peer={}",
                                event.id, event.address_hash
                            );
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    eprintln!("[bridge-tp] link event channel closed, stopping listener");
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("[bridge-tp] link event channel lagged by {} messages", n);
                    continue;
                }
            }
        }
    });

    Ok(address_hash)
}