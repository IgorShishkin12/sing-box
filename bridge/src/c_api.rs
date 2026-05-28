use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing_subscriber::layer::SubscriberExt as _;

use crate::config;
use crate::listener::Listener;
use crate::runtime;
use crate::store::global_store;

// ---------------------------------------------------------------------------
// Callback globals (function pointers stored as usize for Send+Sync)
// ---------------------------------------------------------------------------

pub(crate) static ON_ACCEPT:  AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_CONNECT: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_DATA:    AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_CLOSE:   AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_LOG:     AtomicUsize = AtomicUsize::new(0);

pub(crate) fn call_on_accept(listener_id: u64, conn_id: u64, peer_hash: &str) {
    let ptr = ON_ACCEPT.load(Ordering::Relaxed);
    if ptr == 0 { return; }
    let f: extern "C" fn(u64, u64, *const c_char) = unsafe { std::mem::transmute(ptr) };
    match std::ffi::CString::new(peer_hash) {
        Ok(s) => unsafe { f(listener_id, conn_id, s.as_ptr()) },
        Err(_) => unsafe { f(listener_id, conn_id, b"\0".as_ptr() as *const c_char) },
    }
}

pub(crate) fn call_on_connect(task_id: u64, conn_id: u64) {
    let ptr = ON_CONNECT.load(Ordering::Relaxed);
    if ptr == 0 { return; }
    let f: extern "C" fn(u64, u64) = unsafe { std::mem::transmute(ptr) };
    unsafe { f(task_id, conn_id) };
}

pub(crate) fn call_on_data(conn_id: u64, data: &[u8]) {
    let ptr = ON_DATA.load(Ordering::Relaxed);
    if ptr == 0 { return; }
    let f: extern "C" fn(u64, *const u8, usize) = unsafe { std::mem::transmute(ptr) };
    unsafe { f(conn_id, data.as_ptr(), data.len()) };
}

pub(crate) fn call_on_close(conn_id: u64) {
    let ptr = ON_CLOSE.load(Ordering::Relaxed);
    if ptr == 0 { return; }
    let f: extern "C" fn(u64) = unsafe { std::mem::transmute(ptr) };
    unsafe { f(conn_id) };
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Initialize the bridge with a JSON config and four event callbacks.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_init(
    config_json:  *const c_char,
    on_accept:    Option<extern "C" fn(u64, u64, *const c_char)>,
    on_connect:   Option<extern "C" fn(u64, u64)>,
    on_data:      Option<extern "C" fn(u64, *const u8, usize)>,
    on_close:     Option<extern "C" fn(u64)>,
) -> i32 {
    // Route log:: macro calls into tracing so reticulum-rs events and bridge
    // events all flow through the same subscriber.
    let _ = tracing_log::LogTracer::init();
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default()
            .with(crate::logger::CLogLayer),
    );

    // Store callbacks.
    if let Some(f) = on_accept  { ON_ACCEPT.store(f as usize, Ordering::Relaxed); }
    if let Some(f) = on_connect { ON_CONNECT.store(f as usize, Ordering::Relaxed); }
    if let Some(f) = on_data    { ON_DATA.store(f as usize, Ordering::Relaxed); }
    if let Some(f) = on_close   { ON_CLOSE.store(f as usize, Ordering::Relaxed); }

    if config_json.is_null() {
        log::error!("[bridge] config_json must not be null");
        return -1;
    }
    let c_str = match unsafe { CStr::from_ptr(config_json).to_str() } {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match config::parse_config(c_str) {
        Ok(cfg) => crate::set_global_config(Some(cfg)),
        Err(e) => {
            log::error!("[bridge] config parse error: {}", e);
            return -1;
        }
    }

    if let Err(e) = runtime::init_runtime() {
        log::error!("[bridge] runtime init failed: {}", e);
        return -1;
    }

    if let Some(cfg) = crate::get_global_config() {
        if let Err(e) = crate::transport::init_transport(cfg) {
            log::error!("[bridge] init_transport failed: {}", e);
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

/// Shut down the bridge and release all resources.
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

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Register a named service listener. Blocks until ready.
/// Returns listener_id (> 0) on success, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_listen(listen_hash: *const c_char) -> i64 {
    if listen_hash.is_null() { return -1; }
    let dest = match unsafe { CStr::from_ptr(listen_hash).to_str() } {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let store = global_store();

    let result: Result<u64, String> = runtime::block_on(async move {
        let listen_name = dest;

        crate::transport::get_transport().ok_or_else(|| {
            "reticulum transport not initialized; call reticulum_init first".to_string()
        })?;

        let cfg_dir = crate::get_global_config()
            .map(crate::transport::config_dir_path)
            .ok_or_else(|| "config not initialized".to_string())?;

        let service_identity =
            crate::transport::load_or_create_service_identity(&cfg_dir, &listen_name)
                .map_err(|e| e)?;

        let listener = Listener::with_hash(listen_name.clone());
        let listener_arc = Arc::new(listener);

        let (service_hash, service_dest_arc) =
            crate::transport::register_listener_destination(
                listener_arc.clone(),
                service_identity,
                "sing-box-reticulum".to_string(),
                format!("service.{}", listen_name),
            )
            .await
            .map_err(|e| e.to_string())?;

        listener_arc.set_destination_hash(service_hash).await;

        if let Err(e) = crate::transport::register_discovery_destination(
            listen_name.clone(),
            service_dest_arc.clone(),
            service_hash,
        )
        .await
        {
            log::warn!("[c_api] discovery dest registration failed: {}", e);
        }

        // Announce loop (auto-stop after 600 s).
        {
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let dest_clone = service_dest_arc.clone();
            let name_clone = listen_name.clone();
            tokio::spawn(async move {
                crate::transport::start_service_announce_loop(dest_clone, name_clone, stop_rx).await;
            });
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                let _ = stop_tx.send(true);
            });
        }

        let handle = store.insert_listener((*listener_arc).clone()).await;
        Ok(handle)
    });

    match result {
        Ok(handle) => handle as i64,
        Err(e) => {
            log::error!("[c_api] reticulum_listen failed: {}", e);
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

/// Initiate a connection; calls on_connect(task_id, conn_id) when done.
/// conn_id == 0 on failure.
#[no_mangle]
pub extern "C" fn reticulum_dial(task_id: u64, destination_hash: *const c_char) {
    if destination_hash.is_null() {
        call_on_connect(task_id, 0);
        return;
    }
    let dest = match unsafe { CStr::from_ptr(destination_hash).to_str() } {
        Ok(s) => s.to_string(),
        Err(_) => { call_on_connect(task_id, 0); return; }
    };

    let store = global_store();

    runtime::block_on(async move {
        tokio::spawn(async move {
            let transport = match crate::transport::get_transport() {
                Some(t) => t,
                None => { call_on_connect(task_id, 0); return; }
            };

            let data_rx = {
                let tp = transport.lock().await;
                tp.received_data_events()
            };

            match crate::transport::dial_and_wait(&dest).await {
                Ok((link, link_id)) => {
                    let peer_hash = {
                        let guard = link.lock().await;
                        guard.destination().address_hash.to_hex_string()
                    };
                    let conn = crate::connection::Connection::new_from_link(link, link_id, peer_hash.clone());
                    let handle = store.insert_connection(conn).await;
                    crate::transport::spawn_link_data_reader(handle, link_id, data_rx);
                    call_on_connect(task_id, handle);
                }
                Err(e) => {
                    log::warn!("[c_api] dial failed: {}", e);
                    call_on_connect(task_id, 0);
                }
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Data transfer
// ---------------------------------------------------------------------------

/// Write data to a connection. Returns bytes written, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_write(conn_handle: u64, data: *const u8, len: usize) -> i32 {
    if data.is_null() || len == 0 { return -1; }
    let store = global_store();
    let data_slice = unsafe { std::slice::from_raw_parts(data, len) };

    runtime::block_on(async move {
        match store.get_connection(conn_handle).await {
            Some(conn) => match conn.write(data_slice).await {
                Ok(n) => n as i32,
                Err(e) => {
                    log::warn!("[c_api] write on handle {}: {}", conn_handle, e);
                    -1
                }
            },
            None => -1,
        }
    })
}

/// Close a connection or listener handle.
#[no_mangle]
pub extern "C" fn reticulum_close(handle: u64) {
    let store = global_store();
    runtime::block_on(async move {
        store.remove(handle).await;
    });
    // on_close is fired by the data reader task when the channel closes naturally,
    // but force-fire it here too so Go always gets notified.
    call_on_close(handle);
}

// ---------------------------------------------------------------------------
// Name resolution
// ---------------------------------------------------------------------------

/// Resolve a service name to its address hash. Blocks up to ~30 s.
/// Returns a malloc'd hex string; caller must free with reticulum_free.
#[no_mangle]
pub extern "C" fn reticulum_resolve_name(name: *const c_char) -> *mut c_char {
    if name.is_null() { return std::ptr::null_mut(); }
    let name_str = match unsafe { CStr::from_ptr(name).to_str() } {
        Ok(s) if !s.is_empty() => s.to_string(),
        _ => return std::ptr::null_mut(),
    };

    use std::time::Duration;
    const ANNOUNCE_WAIT: Duration = Duration::from_secs(8);
    const MAX_ATTEMPTS: u32 = 3;
    const INITIAL_BACKOFF: Duration = Duration::from_secs(3);

    let hash_opt = runtime::block_on(async move {
        let mut backoff = INITIAL_BACKOFF;
        for attempt in 0u32..MAX_ATTEMPTS {
            if attempt > 0 {
                log::info!(
                    "[c_api] no service announce for '{}' (attempt {}/{}), retrying in {:?}",
                    name_str, attempt, MAX_ATTEMPTS, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }

            let knock_name = name_str.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Err(e) = crate::transport::dial_discovery_and_wait(&knock_name).await {
                    log::warn!("[c_api] discovery knock failed: {}", e);
                }
            });

            if let Some(hash) = crate::transport::wait_for_service_announce(&name_str, ANNOUNCE_WAIT).await {
                log::info!("[c_api] resolved '{}' → {}", name_str, hash);
                return Some(hash);
            }
        }
        log::warn!("[c_api] gave up resolving '{}' after {} attempts", name_str, MAX_ATTEMPTS);
        None
    });

    match hash_opt {
        Some(hash_str) => {
            let bytes = hash_str.as_bytes();
            let len = bytes.len() + 1;
            unsafe {
                let ptr = libc::malloc(len) as *mut c_char;
                if ptr.is_null() { return std::ptr::null_mut(); }
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
                *ptr.add(bytes.len()) = 0;
                ptr
            }
        }
        None => std::ptr::null_mut(),
    }
}

/// Free a string returned by reticulum_resolve_name.
#[no_mangle]
pub extern "C" fn reticulum_free(ptr: *mut u8) {
    if !ptr.is_null() {
        unsafe { libc::free(ptr as *mut libc::c_void); }
    }
}
