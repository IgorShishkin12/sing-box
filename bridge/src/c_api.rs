use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::config;
use crate::listener::Listener;
use crate::runtime;
use crate::store::global_store;
use crate::task::{global_registry, TaskResult};
use tracing_subscriber::layer::SubscriberExt as _;

pub(crate) static ON_LOG: AtomicUsize = AtomicUsize::new(0);

/// Initialize the reticulum bridge with a JSON config string.
/// Returns 0 on success, -1 on error.
/// config_json must not be NULL; always provide at least "{}" for defaults.
#[no_mangle]
pub extern "C" fn reticulum_init(config_json: *const c_char) -> i32 {
    // Route log:: macro calls into tracing so reticulum-rs events and bridge
    // events all flow through the same subscriber.
    let _ = tracing_log::LogTracer::init();
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default().with(crate::logger::CLogLayer),
    );

    if config_json.is_null() {
        log::error!("config_json must not be null; pass at least {{}} for defaults");
        return -1;
    }
    let c_str = match unsafe { CStr::from_ptr(config_json).to_str() } {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match config::parse_config(c_str) {
        Ok(cfg) => {
            crate::set_global_config(Some(cfg));
        }
        Err(e) => {
            log::error!("config parse error: {}", e);
            return -1;
        }
    }

    if let Err(e) = runtime::init_runtime() {
        log::error!("runtime init failed: {}", e);
        return -1;
    }

    if let Some(cfg) = crate::get_global_config() {
        if let Err(e) = crate::transport::init_transport(cfg) {
            log::error!("init_transport failed: {}", e);
            return -1;
        }
    }

    0
}

/// Register the Go log callback. Must be called before `reticulum_init` to
/// capture early initialisation log events. Safe to call from any thread.
#[no_mangle]
pub extern "C" fn reticulum_set_log_callback(
    on_log: Option<extern "C" fn(u8, *const c_char, *const c_char)>,
) {
    if let Some(f) = on_log {
        ON_LOG.store(f as usize, Ordering::Relaxed);
    }
}


/// Get the destination hash for a given name.
/// The caller must free `*hash` with reticulum_free after use.
/// Returns 0 on success, -1 if the name is unknown.
#[no_mangle]
pub extern "C" fn get_hash(hash: *mut *mut c_char, name: *const c_char) -> i32 {
    if hash.is_null() || name.is_null() {
        return -1;
    }
    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let store = global_store();
    let result = runtime::block_on(async move { store.get_hash_for_name(&name_str).await });

    match result {
        Some(hash_str) => {
            let bytes = hash_str.as_bytes();
            let len = bytes.len() + 1;
            unsafe {
                let ptr = libc::malloc(len) as *mut c_char;
                if ptr.is_null() {
                    return -1;
                }
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
                *ptr.add(bytes.len()) = 0;
                *hash = ptr;
            }
            0
        }
        None => -1,
    }
}

/// Register a name→hash mapping for later lookup via get_hash.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_register_name(name: *const c_char, hash: *const c_char) -> i32 {
    if name.is_null() || hash.is_null() {
        return -1;
    }
    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };
    let hash_str = match unsafe { CStr::from_ptr(hash) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let store = global_store();
    runtime::block_on(async move {
        store.register_name(&name_str, &hash_str).await;
    });
    0
}

/// Shutdown the bridge and release resources.
///
/// Clears the store so that old handles become invalid.
/// The Tokio runtime itself is kept alive to avoid thread-local
/// state corruption from dropping and recreating it.
/// Note: the Android logger (init_once) is process-wide and has no shutdown API.
#[no_mangle]
pub extern "C" fn reticulum_shutdown() {
    if runtime::has_runtime() {
        let store = global_store();
        runtime::block_on(async move {
            store.clear_all().await;
        });
    }
    runtime::shutdown();
}

/// Dial a destination hash, returning a task ID.
/// Use reticulum_poll to check for completion and get the connection handle.
#[no_mangle]
pub extern "C" fn reticulum_dial(destination_hash: *const c_char) -> i32 {
    if destination_hash.is_null() {
        return -1;
    }
    let dest = match unsafe { CStr::from_ptr(destination_hash) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let registry = global_registry();
    let store = global_store();

    let task_id = runtime::block_on(async { registry.insert_pending().await });

    runtime::block_on(async move {
        match crate::transport::get_transport() {
            Some(transport) => {
                // Subscribe to data events BEFORE dialing so we capture packets
                // that arrive the instant the link activates on the server side.
                let (data_rx, link_closed_rx) = {
                    let tp = transport.lock().await;
                    (tp.received_data_events(), tp.out_link_events())
                };
                match crate::transport::dial_and_wait(&dest).await {
                    Ok((link, link_id)) => {
                        let conn = crate::connection::Connection::new_from_link(link, link_id);
                        crate::transport::spawn_link_data_reader(conn.clone(), link_id, data_rx, link_closed_rx);
                        let handle = store.insert_connection(conn).await;
                        registry
                            .complete(
                                task_id,
                                TaskResult::Done {
                                    handle,
                                    data: vec![],
                                },
                            )
                            .await;
                    }
                    Err(e) => {
                        registry
                            .complete(
                                task_id,
                                TaskResult::Error {
                                    message: format!("dial failed: {}", e),
                                },
                            )
                            .await;
                    }
                }
            }
            None => {
                registry
                    .complete(
                        task_id,
                        TaskResult::Error {
                            message:
                                "reticulum transport not initialized; call reticulum_init first"
                                    .to_string(),
                        },
                    )
                    .await;
            }
        }
    });

    task_id as i32
}

/// Listen on a name, returning a task ID.
/// Use reticulum_poll to check for completion and get the listener handle.
#[no_mangle]
pub extern "C" fn reticulum_listen(listen_hash: *const c_char) -> i32 {
    if listen_hash.is_null() {
        return -1;
    }
    let dest = match unsafe { CStr::from_ptr(listen_hash) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let registry = global_registry();
    let store = global_store();

    let task_id = runtime::block_on(async { registry.insert_pending().await });

    runtime::block_on(async move {
        let listen_name = dest;
        match crate::transport::get_transport() {
            Some(_) => {
                let cfg_dir = match crate::get_global_config() {
                    Some(cfg) => crate::transport::config_dir_path(cfg),
                    None => {
                        registry.complete(
                            task_id,
                            TaskResult::Error {
                                message: "reticulum transport not initialized; call reticulum_init first".to_string(),
                            },
                        ).await;
                        return;
                    }
                };

                let service_identity =
                    match crate::transport::load_or_create_service_identity(&cfg_dir, &listen_name)
                    {
                        Ok(id) => id,
                        Err(e) => {
                            registry
                                .complete(task_id, TaskResult::Error { message: e })
                                .await;
                            return;
                        }
                    };

                let listener = Listener::with_hash(listen_name.clone());
                let listener_arc = Arc::new(listener);

                match crate::transport::register_listener_destination(
                    listener_arc.clone(),
                    service_identity,
                    "sing-box-reticulum".to_string(),
                    format!("service.{}", listen_name),
                )
                .await
                {
                    Ok((service_hash, service_dest_arc)) => {
                        listener_arc.set_destination_hash(service_hash).await;

                        if let Err(e) = crate::transport::register_discovery_destination(
                            listen_name.clone(),
                            service_dest_arc.clone(),
                            service_hash,
                        )
                        .await
                        {
                            log::warn!("discovery dest registration failed: {}", e);
                        }

                        {
                            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
                            let dest_clone = service_dest_arc.clone();
                            let name_clone = listen_name.clone();
                            tokio::spawn(async move {
                                crate::transport::start_service_announce_loop(
                                    dest_clone, name_clone, stop_rx,
                                )
                                .await;
                            });
                            tokio::spawn(async move {
                                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                                let _ = stop_tx.send(true);
                            });
                        }

                        let handle = store.insert_listener((*listener_arc).clone()).await;
                        registry
                            .complete(
                                task_id,
                                TaskResult::Done {
                                    handle,
                                    data: vec![],
                                },
                            )
                            .await;
                    }
                    Err(e) => {
                        registry
                            .complete(
                                task_id,
                                TaskResult::Error {
                                    message: format!(
                                        "failed to register service destination: {}",
                                        e
                                    ),
                                },
                            )
                            .await;
                    }
                }
            }
            None => {
                registry
                    .complete(
                        task_id,
                        TaskResult::Error {
                            message:
                                "reticulum transport not initialized; call reticulum_init first"
                                    .to_string(),
                        },
                    )
                    .await;
            }
        }
    });

    task_id as i32
}

/// Get the address hash of a listener as a hex string.
/// The caller must free the returned string with reticulum_free.
/// Returns NULL if the listener is not found or has no hash.
#[no_mangle]
pub extern "C" fn reticulum_get_listener_hash(listener_handle: u64) -> *mut c_char {
    let store = global_store();
    let result: Option<String> = runtime::block_on(async move {
        match store.get_listener(listener_handle).await {
            Some(listener) => {
                let hash = listener.destination_hash().await;
                match hash {
                    Some(h) => Some(h.to_hex_string()),
                    None => None,
                }
            }
            None => None,
        }
    });
    match result {
        Some(hash_str) => {
            let bytes = hash_str.as_bytes();
            let len = bytes.len() + 1;
            unsafe {
                let ptr = libc::malloc(len) as *mut c_char;
                if ptr.is_null() {
                    return std::ptr::null_mut();
                }
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
                *ptr.add(bytes.len()) = 0;
                ptr
            }
        }
        None => std::ptr::null_mut(),
    }
}

/// Close a connection or listener handle.
/// For connections, also closes the underlying Reticulum link so that the next
/// dial to the same destination creates a new link (and triggers a new server accept event).
#[no_mangle]
pub extern "C" fn reticulum_close(handle: u64) {
    let store = global_store();
    runtime::block_on(async move {
        match store.remove(handle).await {
            Some(crate::store::StoreEntry::Connection(conn)) => {
                log::info!("reticulum_close: closing connection handle={}, closing link", handle);
                conn.close_link().await;
            }
            Some(crate::store::StoreEntry::Listener(_)) => {
                log::info!("reticulum_close: closing listener handle={}", handle);
            }
            None => {
                log::warn!("reticulum_close: handle={} not found in store", handle);
            }
        }
    });
}

/// Accept a pending connection from a listener.
/// Returns a task ID. Use reticulum_poll to get the new connection handle.
/// The task completes when a connection arrives or after a 300-second timeout.
#[no_mangle]
pub extern "C" fn reticulum_accept(listener_handle: u64) -> i32 {
    let registry = global_registry();
    let store = global_store();

    let task_id = runtime::block_on(async { registry.insert_pending().await });

    runtime::block_on(async move {
        tokio::spawn(async move {
            const ACCEPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
            match store.get_listener(listener_handle).await {
                Some(listener) => match listener.accept_wait(ACCEPT_TIMEOUT).await {
                    Some(conn) => {
                        let handle = store.insert_connection(conn).await;
                        log::debug!("accept: listener={} → conn handle={}", listener_handle, handle);
                        registry
                            .complete(
                                task_id,
                                TaskResult::Done {
                                    handle,
                                    data: vec![],
                                },
                            )
                            .await;
                    }
                    None => {
                        log::warn!("accept: timeout on listener={}", listener_handle);
                        registry
                            .complete(
                                task_id,
                                TaskResult::Error {
                                    message: "accept timeout".to_string(),
                                },
                            )
                            .await;
                    }
                },
                None => {
                    log::warn!("accept: invalid listener handle={}", listener_handle);
                    registry
                        .complete(
                            task_id,
                            TaskResult::Error {
                                message: "invalid listener handle".to_string(),
                            },
                        )
                        .await;
                }
            }
        });
    });

    task_id as i32
}

/// Write data to a connection.
/// Returns number of bytes written, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_write(conn_handle: u64, data: *const u8, len: usize) -> i32 {
    if data.is_null() || len == 0 {
        return -1;
    }
    let store = global_store();
    let data_slice = unsafe { std::slice::from_raw_parts(data, len) };

    runtime::block_on(async move {
        match store.get_connection(conn_handle).await {
            Some(conn) => match conn.write(data_slice).await {
                Ok(n) => n as i32,
                Err(e) => {
                    log::warn!("write on handle {}: {}", conn_handle, e);
                    -1
                }
            },
            None => {
                log::warn!("write: invalid conn handle={}", conn_handle);
                -1
            }
        }
    })
}

/// Read data from a connection.
/// Returns number of bytes read, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_read(conn_handle: u64, buffer: *mut u8, max_len: usize) -> i32 {
    if buffer.is_null() || max_len == 0 {
        return -1;
    }
    let store = global_store();
    let buffer_slice = unsafe { std::slice::from_raw_parts_mut(buffer, max_len) };

    runtime::block_on(async move {
        match store.get_connection(conn_handle).await {
            Some(conn) => conn.read(buffer_slice).await, // i32: >0 bytes, 0 no data, -1 EOF
            None => {
                log::warn!("read: invalid conn handle={}", conn_handle);
                -1
            }
        }
    })
}

/// Allocate a buffer with libc::malloc and copy bytes into it.
unsafe fn alloc_with_libc(data: &[u8]) -> *mut u8 {
    let len = data.len();
    let ptr = libc::malloc(len) as *mut u8;
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
    ptr
}

/// Poll for completion of a task.
/// Returns 0=pending, 1=done, -1=error.
/// If done, the result handle is stored in *result_out and its length in *len_out.
/// The caller must free the result with reticulum_free.
#[no_mangle]
pub extern "C" fn reticulum_poll(
    task_id: i32,
    result_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    if task_id < 0 {
        log::warn!("poll: invalid task_id={}", task_id);
        return -1;
    }
    let registry = global_registry();
    let result = runtime::block_on(async move { registry.get_and_remove(task_id as u64).await });
    match result {
        None => 0,
        Some(TaskResult::Done { handle, data: _ }) => {
            log::debug!("poll: task={} done, handle={}", task_id, handle);
            let handle_bytes = handle.to_le_bytes();
            let len = handle_bytes.len();
            unsafe {
                let ptr = alloc_with_libc(&handle_bytes);
                if ptr.is_null() {
                    return -1;
                }
                if !result_out.is_null() {
                    *result_out = ptr;
                }
                if !len_out.is_null() {
                    *len_out = len;
                }
            }
            1
        }
        Some(TaskResult::Error { message }) => {
            log::warn!("poll: task={} error: {}", task_id, message);
            let msg_bytes = message.into_bytes();
            let len = msg_bytes.len();
            unsafe {
                let ptr = alloc_with_libc(&msg_bytes);
                if ptr.is_null() {
                    return -1;
                }
                if !result_out.is_null() {
                    *result_out = ptr;
                }
                if !len_out.is_null() {
                    *len_out = len;
                }
            }
            -1
        }
    }
}

/// Free memory allocated by the bridge.
#[no_mangle]
pub extern "C" fn reticulum_free(ptr: *mut u8) {
    if !ptr.is_null() {
        unsafe {
            libc::free(ptr as *mut libc::c_void);
        }
    }
}

/// Resolve a human-readable service name to its address hash via the Reticulum network.
///
/// Repeatedly knocks on the discovery destination and waits for the server to announce
/// the real service hash. Up to 3 attempts with exponential backoff (3s → 6s → 12s).
///
/// The caller must free the returned string with reticulum_free.
/// Returns NULL on timeout or if transport is not initialized.
#[no_mangle]
pub extern "C" fn reticulum_resolve_name(name: *const c_char) -> *mut c_char {
    if name.is_null() {
        return std::ptr::null_mut();
    }
    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) if !s.is_empty() => s.to_string(),
        _ => return std::ptr::null_mut(),
    };

    use std::time::Duration;

    // Server re-announces every 5s; 8s gives one full cycle as margin.
    const ANNOUNCE_WAIT: Duration = Duration::from_secs(8);
    const MAX_ATTEMPTS: u32 = 3;
    const INITIAL_BACKOFF: Duration = Duration::from_secs(3);

    let hash_opt = runtime::block_on(async move {
        let mut backoff = INITIAL_BACKOFF;
        for attempt in 0u32..MAX_ATTEMPTS {
            if attempt > 0 {
                log::info!(
                    "no service announce for '{}' (attempt {}/{}), retrying in {:?}",
                    name_str,
                    attempt,
                    MAX_ATTEMPTS,
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }

            let knock_name = name_str.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Err(e) = crate::transport::dial_discovery_and_wait(&knock_name).await {
                    log::warn!("discovery knock failed: {}", e);
                }
            });

            if let Some(hash) =
                crate::transport::wait_for_service_announce(&name_str, ANNOUNCE_WAIT).await
            {
                log::info!("resolved '{}' → {}", name_str, hash);
                return Some(hash);
            }
        }
        log::warn!(
            "gave up resolving '{}' after {} attempts",
            name_str,
            MAX_ATTEMPTS
        );
        None
    });

    match hash_opt {
        Some(hash_str) => {
            let bytes = hash_str.as_bytes();
            let len = bytes.len() + 1;
            unsafe {
                let ptr = libc::malloc(len) as *mut c_char;
                if ptr.is_null() {
                    return std::ptr::null_mut();
                }
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
                *ptr.add(bytes.len()) = 0;
                ptr
            }
        }
        None => std::ptr::null_mut(),
    }
}
