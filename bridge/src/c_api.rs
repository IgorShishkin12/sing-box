use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::config;
use crate::listener::Listener;
use crate::runtime;
use crate::store::global_store;
use tracing_subscriber::layer::SubscriberExt as _;

// ---------------------------------------------------------------------------
// Callback slots
// ---------------------------------------------------------------------------

pub(crate) static ON_LOG: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_ACCEPT: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_CONNECT: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_DATA: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_CLOSE: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_RESOLVE: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ON_WRITE: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn call_on_accept(listener_id: u64, conn_id: u64, peer_hash: &str) {
    let f_ptr = ON_ACCEPT.load(Ordering::Relaxed);
    if f_ptr != 0 {
        let f: extern "C" fn(u64, u64, *const c_char) = unsafe { std::mem::transmute(f_ptr) };
        let peer_hash_c = CString::new(peer_hash).unwrap_or_default();
        f(listener_id, conn_id, peer_hash_c.as_ptr());
    }
}

pub(crate) fn call_on_connect(task_id: u64, conn_id: u64) {
    let f_ptr = ON_CONNECT.load(Ordering::Relaxed);
    if f_ptr != 0 {
        let f: extern "C" fn(u64, u64) = unsafe { std::mem::transmute(f_ptr) };
        f(task_id, conn_id);
    }
}

pub(crate) fn call_on_data(conn_id: u64, data: &[u8]) {
    let f_ptr = ON_DATA.load(Ordering::Relaxed);
    if f_ptr != 0 {
        let f: extern "C" fn(u64, *const u8, usize) = unsafe { std::mem::transmute(f_ptr) };
        f(conn_id, data.as_ptr(), data.len());
    }
}

pub(crate) fn call_on_close(conn_id: u64) {
    let f_ptr = ON_CLOSE.load(Ordering::Relaxed);
    if f_ptr != 0 {
        let f: extern "C" fn(u64) = unsafe { std::mem::transmute(f_ptr) };
        f(conn_id);
    }
}

pub(crate) fn call_on_resolve(task_id: u64, hash: Option<String>) {
    let f_ptr = ON_RESOLVE.load(Ordering::Relaxed);
    if f_ptr != 0 {
        let f: extern "C" fn(u64, *const c_char) = unsafe { std::mem::transmute(f_ptr) };
        let ptr = alloc_c_string(hash);
        f(task_id, ptr);
    }
}

pub(crate) fn call_on_write(task_id: u64, bytes: i32) {
    let f_ptr = ON_WRITE.load(Ordering::Relaxed);
    if f_ptr != 0 {
        let f: extern "C" fn(u64, i32) = unsafe { std::mem::transmute(f_ptr) };
        f(task_id, bytes);
    }
}

// ---------------------------------------------------------------------------
// Init / shutdown
// ---------------------------------------------------------------------------

/// Initialize the reticulum bridge.
/// `config_json` must not be NULL; pass at least `"{}"` for defaults.
/// The four callback pointers are called from Rust tokio threads — they must
/// be safe to call from a non-Go-started OS thread (keep them minimal: just
/// write to a channel and return).
/// Returns 0 on success, -1 on error.
/// config_json must not be NULL; always provide at least "{}" for defaults.
///
/// # Safety
/// `config_json` must be a valid, non-null, null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn reticulum_init(
    config_json: *const c_char,
    on_accept: Option<extern "C" fn(u64, u64, *const c_char)>,
    on_connect: Option<extern "C" fn(u64, u64)>,
    on_data: Option<extern "C" fn(u64, *const u8, usize)>,
    on_close: Option<extern "C" fn(u64)>,
) -> i32 {
    let _ = tracing_log::LogTracer::init();
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default().with(crate::logger::CLogLayer),
    );

    if let Some(f) = on_accept {
        ON_ACCEPT.store(f as usize, Ordering::Relaxed);
    }
    if let Some(f) = on_connect {
        ON_CONNECT.store(f as usize, Ordering::Relaxed);
    }
    if let Some(f) = on_data {
        ON_DATA.store(f as usize, Ordering::Relaxed);
    }
    if let Some(f) = on_close {
        ON_CLOSE.store(f as usize, Ordering::Relaxed);
    }

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

/// Register the Go log callback. Must be called before `reticulum_init`.
#[no_mangle]
pub extern "C" fn reticulum_set_log_callback(
    on_log: Option<extern "C" fn(u8, *const c_char, *const c_char)>,
) {
    if let Some(f) = on_log {
        ON_LOG.store(f as usize, Ordering::Relaxed);
    }
}

/// Register the name-resolution callback.
/// Called from Rust with `(task_id, hash_ptr)` when `reticulum_resolve_name` completes.
/// `hash_ptr` is NULL on timeout; when non-NULL it is malloc'd and must be freed
/// with `reticulum_free`.
#[no_mangle]
pub extern "C" fn reticulum_set_resolve_callback(
    on_resolve: Option<extern "C" fn(u64, *const c_char)>,
) {
    if let Some(f) = on_resolve {
        ON_RESOLVE.store(f as usize, Ordering::Relaxed);
    }
}

/// Register the write-completion callback.
/// Called from Rust with `(task_id, bytes)` when `reticulum_write` completes.
/// `bytes` is the number of bytes written, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_set_write_callback(on_write: Option<extern "C" fn(u64, i32)>) {
    if let Some(f) = on_write {
        ON_WRITE.store(f as usize, Ordering::Relaxed);
    }
}

/// Shutdown the bridge and release resources.
#[no_mangle]
pub extern "C" fn reticulum_shutdown() {
    if runtime::has_runtime() {
        let store = global_store();
        runtime::block_on(async move {
            store.clear_all().await;
        });
    }
    crate::transport::clear_transport();
    runtime::shutdown();
    // Zero all callback pointers last, after tasks are aborted and the runtime
    // is dropped, so no surviving task can invoke a dangling function pointer.
    ON_ACCEPT.store(0, Ordering::SeqCst);
    ON_CONNECT.store(0, Ordering::SeqCst);
    ON_DATA.store(0, Ordering::SeqCst);
    ON_CLOSE.store(0, Ordering::SeqCst);
    ON_RESOLVE.store(0, Ordering::SeqCst);
    ON_WRITE.store(0, Ordering::SeqCst);
    ON_LOG.store(0, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------
// Dial / Listen
// ---------------------------------------------------------------------------

/// Dial a destination hash. Non-blocking; fires `on_connect(task_id, conn_id)`
/// when done (`conn_id == 0` on failure).
/// # Safety
/// `destination_hash` must be a valid, non-null, null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn reticulum_dial(task_id: u64, destination_hash: *const c_char) {
    if destination_hash.is_null() {
        call_on_connect(task_id, 0);
        return;
    }
    let dest = match unsafe { CStr::from_ptr(destination_hash) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            call_on_connect(task_id, 0);
            return;
        }
    };

    if !runtime::has_runtime() {
        log::warn!("dial: bridge not initialized");
        call_on_connect(task_id, 0);
        return;
    }

    let store = global_store();

    let dial_handle = runtime::spawn(async move {
        let transport = match crate::transport::get_transport() {
            Some(t) => t,
            None => {
                log::error!("dial: transport not initialized");
                call_on_connect(task_id, 0);
                return;
            }
        };

        // Subscribe before dialing so events buffered during link activation
        // are not missed.
        let data_rx = {
            let tp = transport.lock().await;
            tp.received_data_events()
        };
        let resource_rx = {
            let tp = transport.lock().await;
            tp.resource_events()
        };
        let mut link_events = {
            let tp = transport.lock().await;
            tp.out_link_events()
        };

        match crate::transport::dial_and_wait(&dest).await {
            Ok((link, link_id)) => {
                let peer_hash = { link.lock().await.peer_identity().address_hash };
                log::info!("outbound link active: id={} peer={}", link_id, peer_hash);
                let identified =
                    crate::transport::exchange_identify_on_link(&link, link_id, &mut link_events)
                        .await;
                let conn = crate::connection::Connection::new_from_link(
                    link,
                    link_id,
                    Some(peer_hash),
                    identified,
                );
                let conn_id = store.insert_connection(conn).await;
                crate::transport::spawn_link_data_reader(conn_id, link_id, data_rx);
                crate::transport::spawn_resource_event_reader(conn_id, link_id, resource_rx);
                crate::transport::open_channel_and_forward(conn_id, link_id).await;
                call_on_connect(task_id, conn_id);
            }
            Err(e) => {
                log::error!("dial failed: {}", e);
                call_on_connect(task_id, 0);
            }
        }
    });
    runtime::register_task(dial_handle);
}

/// Listen on a hash. Blocks until the listener is registered.
/// Returns the listener handle (>0) on success, -1 on error.
/// # Safety
/// `listen_hash` must be a valid, non-null, null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn reticulum_listen(listen_hash: *const c_char) -> i64 {
    if listen_hash.is_null() {
        return -1;
    }
    let dest = match unsafe { CStr::from_ptr(listen_hash) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let store = global_store();

    let result: Result<u64, String> = runtime::block_on(async move {
        let listen_name = dest;
        match crate::transport::get_transport() {
            Some(_) => {}
            None => return Err("transport not initialized".to_string()),
        }

        let cfg_dir = match crate::get_global_config() {
            Some(cfg) => crate::transport::config_dir_path(cfg),
            None => return Err("config not set".to_string()),
        };

        let service_identity =
            crate::transport::load_or_create_service_identity(&cfg_dir, &listen_name)?;

        let listener = Listener::with_hash(listen_name.clone());
        let listener_arc = Arc::new(listener);
        let handle = store.insert_listener((*listener_arc).clone()).await;

        match crate::transport::register_listener_destination(
            handle,
            listener_arc.clone(),
            service_identity,
            "sing-box-reticulum".to_string(),
            format!("service.{}", listen_name),
        )
        .await
        {
            Ok((service_hash, service_dest_arc)) => {
                listener_arc.set_destination_hash(service_hash).await;
                {
                    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
                    let dest_clone = service_dest_arc.clone();
                    let name_clone = listen_name.clone();
                    let ann_handle = tokio::spawn(async move {
                        crate::transport::start_service_announce_loop(
                            dest_clone, name_clone, stop_rx,
                        )
                        .await;
                    });
                    runtime::register_task(ann_handle);
                }
                Ok(handle)
            }
            Err(e) => Err(format!("failed to register service destination: {}", e)),
        }
    });

    match result {
        Ok(handle) => {
            log::info!("listen: handle={}", handle);
            handle as i64
        }
        Err(e) => {
            log::error!("listen failed: {}", e);
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Connection I/O
// ---------------------------------------------------------------------------

/// Write data to a connection. Non-blocking.
/// Fires `on_write(task_id, bytes)` when done; `bytes` is -1 on error.
///
/// # Safety
/// `data` must be a valid pointer to at least `len` initialized bytes for the
/// duration of this call. The buffer is copied before the function returns.
#[no_mangle]
pub unsafe extern "C" fn reticulum_write(
    task_id: u64,
    conn_handle: u64,
    data: *const u8,
    len: usize,
) {
    if data.is_null() || len == 0 {
        call_on_write(task_id, -1);
        return;
    }
    // Copy before returning — the caller's buffer may be freed immediately after.
    let data_vec = unsafe { std::slice::from_raw_parts(data, len) }.to_vec();
    let store = global_store();
    runtime::spawn(async move {
        let result = match store.get_connection(conn_handle).await {
            Some(conn) => match conn.write(&data_vec).await {
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
        };
        call_on_write(task_id, result);
    });
}

/// Close a connection or listener handle. Fire-and-forget.
#[no_mangle]
pub extern "C" fn reticulum_close(handle: u64) {
    log::debug!("close handle={}", handle);
    if !runtime::has_runtime() {
        return;
    }
    let store = global_store();
    runtime::spawn(async move {
        store.remove(handle).await;
    });
}

// ---------------------------------------------------------------------------
// Identity / hash getters
// ---------------------------------------------------------------------------

/// Get the destination hash of a listener as a hex string.
/// Caller must free with reticulum_free. Returns NULL if not found.
#[no_mangle]
pub extern "C" fn reticulum_get_listener_hash(listener_handle: u64) -> *mut c_char {
    let store = global_store();
    let result: Option<String> = runtime::block_on(async move {
        match store.get_listener(listener_handle).await {
            Some(listener) => listener.destination_hash().await.map(|h| h.to_hex_string()),
            None => None,
        }
    });
    alloc_c_string(result)
}

/// Get the peer identity hash for a connection. Caller must free with reticulum_free.
#[no_mangle]
pub extern "C" fn reticulum_get_conn_peer_hash(conn_handle: u64) -> *mut c_char {
    let store = global_store();
    let result: Option<String> = runtime::block_on(async move {
        store
            .get_connection(conn_handle)
            .await
            .and_then(|conn| conn.peer_hash())
            .map(|h| h.to_hex_string())
    });
    alloc_c_string(result)
}

/// Get the verified persistent identity hash of the remote peer (from LinkIdentify).
/// Caller must free with reticulum_free.
#[no_mangle]
pub extern "C" fn reticulum_get_conn_identified_peer(conn_handle: u64) -> *mut c_char {
    let store = global_store();
    let result: Option<String> = runtime::block_on(async move {
        store
            .get_connection(conn_handle)
            .await
            .and_then(|conn| conn.identified_peer())
            .map(|h| h.to_hex_string())
    });
    alloc_c_string(result)
}

/// Get the max payload bytes per data_packet the mux may hand to one channel send.
///
/// This is the single-packet plaintext budget
/// (`packet_mdu() - FERNET_OVERHEAD_SIZE - FERNET_MAX_PADDING_SIZE`) minus the
/// Channel envelope (`CHANNEL_ENVELOPE_OVERHEAD`), because every small write now
/// rides the Reticulum Channel, which prepends that envelope inside the encrypted
/// packet. The mux fragments to this value so each fragment fits one channel
/// packet; without the reservation, max-size fragments overflow the packet and
/// `Connection::write` falls back to a Resource per fragment (catastrophically
/// slow on a tight-MTU link). Returns -1 if the connection is not a link or not found.
#[no_mangle]
pub extern "C" fn reticulum_get_conn_max_payload(conn_handle: u64) -> i32 {
    use reticulum_rs::transport::crypt::fernet::{FERNET_MAX_PADDING_SIZE, FERNET_OVERHEAD_SIZE};
    let store = global_store();
    runtime::block_on(async move {
        let conn = match store.get_connection(conn_handle).await {
            Some(c) => c,
            None => return -1,
        };
        let link = match conn.link() {
            Some(l) => l,
            None => return -1,
        };
        let guard = link.lock().await;
        guard
            .packet_mdu()
            .saturating_sub(FERNET_OVERHEAD_SIZE + FERNET_MAX_PADDING_SIZE)
            .saturating_sub(crate::connection::CHANNEL_ENVELOPE_OVERHEAD) as i32
    })
}

/// Get the local transport identity hash. Caller must free with reticulum_free.
#[no_mangle]
pub extern "C" fn reticulum_get_transport_hash() -> *mut c_char {
    alloc_c_string(crate::transport::get_transport_identity_hash())
}

// ---------------------------------------------------------------------------
// Name registry
// ---------------------------------------------------------------------------

/// Register a name→hash mapping. Returns 0 on success, -1 on error.
///
/// # Safety
/// `name` and `hash` must be valid, null-terminated C strings or null pointers.
#[no_mangle]
pub unsafe extern "C" fn reticulum_register_name(name: *const c_char, hash: *const c_char) -> i32 {
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

/// Get the hash for a registered name. Caller must free with reticulum_free.
/// Returns 0 in the out-param on success, -1 if the name is unknown.
///
/// # Safety
/// `hash` must be a valid pointer to a `*mut c_char`. `name` must be a valid
/// null-terminated C string or null pointer.
#[no_mangle]
pub unsafe extern "C" fn get_hash(hash: *mut *mut c_char, name: *const c_char) -> i32 {
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
            let ptr = alloc_c_string(Some(hash_str));
            if ptr.is_null() {
                return -1;
            }
            unsafe {
                *hash = ptr;
            }
            0
        }
        None => -1,
    }
}

/// Resolve a service name to its address hash via network announcements. Non-blocking.
/// Fires `on_resolve(task_id, hash_ptr)` when done.
/// `hash_ptr` is NULL on timeout; when non-NULL it is malloc'd — free with `reticulum_free`.
/// Retries up to 3× with exponential backoff (3 s → 6 s → 12 s), 15 s per attempt.
///
/// # Safety
/// `name` must be a valid, null-terminated C string or NULL.
#[no_mangle]
pub unsafe extern "C" fn reticulum_resolve_name(task_id: u64, name: *const c_char) {
    if name.is_null() {
        call_on_resolve(task_id, None);
        return;
    }
    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) if !s.is_empty() => s.to_string(),
        _ => {
            call_on_resolve(task_id, None);
            return;
        }
    };

    if !runtime::has_runtime() {
        log::warn!("resolve_name: bridge not initialized");
        call_on_resolve(task_id, None);
        return;
    }

    use std::time::Duration;
    const ANNOUNCE_WAIT: Duration = Duration::from_secs(15);
    const MAX_ATTEMPTS: u32 = 3;
    const INITIAL_BACKOFF: Duration = Duration::from_secs(3);

    let handle = runtime::spawn(async move {
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
            if let Some(hash) =
                crate::transport::wait_for_service_announce(&name_str, ANNOUNCE_WAIT).await
            {
                log::info!("resolved '{}' → {}", name_str, hash);
                call_on_resolve(task_id, Some(hash));
                return;
            }
        }
        log::warn!(
            "gave up resolving '{}' after {} attempts",
            name_str,
            MAX_ATTEMPTS
        );
        call_on_resolve(task_id, None);
    });
    runtime::register_task(handle);
}

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

/// Free memory allocated by the bridge.
#[no_mangle]
pub extern "C" fn reticulum_free(ptr: *mut u8) {
    if !ptr.is_null() {
        unsafe {
            libc::free(ptr as *mut libc::c_void);
        }
    }
}

/// Allocate a null-terminated C string via libc malloc. Returns NULL on failure.
fn alloc_c_string(s: Option<String>) -> *mut c_char {
    let s = match s {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };
    let bytes = s.as_bytes();
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
