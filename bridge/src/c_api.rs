use std::ffi::CStr;
use std::os::raw::c_char;

#[cfg(feature = "real-reticulum")]
use std::sync::Arc;

use crate::config;
use crate::connection::{create_pair, Connection};
use crate::listener::Listener;
use crate::runtime;
use crate::store::global_store;
use crate::task::{TaskResult, global_registry};

#[cfg(feature = "real-reticulum")]
use reticulum_rs::transport::identity::PrivateIdentity;

/// Initialize the reticulum bridge with a JSON config string.
/// Returns 0 on success, -1 on error.
/// If config_json is NULL, uses default configuration.
#[no_mangle]
pub extern "C" fn reticulum_init(config_json: *const c_char) -> i32 {
    if !config_json.is_null() {
        let c_str = match unsafe { CStr::from_ptr(config_json).to_str() } {
            Ok(s) => s,
            Err(_) => return -1,
        };
        // Treat empty string as NULL (use default config)
        if c_str.is_empty() {
            crate::set_global_config(None);
        } else {
            match config::parse_config(c_str) {
                Ok(cfg) => {
                    crate::set_global_config(Some(cfg));
                }
                Err(_) => return -1,
            }
        }
    } else {
        crate::set_global_config(None);
    }
    runtime::init_runtime();

    // Initialize real Reticulum transport when the feature is active.
    #[cfg(feature = "real-reticulum")]
    {
        if let Some(cfg) = crate::get_global_config() {
            if crate::transport::init_transport(cfg) != 0 {
                return -1;
            }
        }
    }

    0
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
    let result = runtime::block_on(async move {
        store.get_hash_for_name(&name_str).await
    });

    match result {
        Some(hash_str) => {
            // Allocate with libc so libc::free in reticulum_free works correctly.
            let bytes = hash_str.as_bytes();
            let len = bytes.len() + 1; // NUL terminator
            unsafe {
                let ptr = libc::malloc(len) as *mut c_char;
                if ptr.is_null() {
                    return -1;
                }
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
                *ptr.add(bytes.len()) = 0; // NUL terminator
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
#[no_mangle]
pub extern "C" fn reticulum_shutdown() {
    // Only try to clear the store if we have a runtime — tests may call
    // shutdown without having called init first (e.g. null-param tests).
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
    
    // Create a pending task
    let task_id = runtime::block_on(async {
        registry.insert_pending().await
    });
    
    // Spawn the async dial operation
    runtime::block_on(async move {
        #[cfg(feature = "real-reticulum")]
        {
            // Real Reticulum path: attempt to establish a real link via the transport.
            match crate::transport::get_transport() {
                Some(_) => {
                    match crate::transport::dial_and_wait(&dest).await {
                        Ok((link, link_id)) => {
                            // Create a Connection wrapping the real Link
                            let conn = Connection::new_from_link(link, link_id);
                            // Spawn a background data reader for inbound data on this link
                            crate::transport::spawn_link_data_reader(conn.clone(), link_id);
                            let handle = store.insert_connection(conn).await;
                            registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
                        }
                        Err(e) => {
                            registry.complete(
                                task_id,
                                TaskResult::Error {
                                    message: format!("dial failed: {}", e),
                                },
                            ).await;
                        }
                    }
                }
                None => {
                    // Transport not initialized — fall through to in-memory path below.
                    // (The code after the #[cfg] block handles this case.)
                    // We still complete with the in-memory path.
                    in_memory_dial(&dest, &store, &registry, task_id).await;
                }
            }
        }
        
        #[cfg(not(feature = "real-reticulum"))]
        {
            in_memory_dial(&dest, &store, &registry, task_id).await;
        }
    });
    
    task_id as i32
}

/// In-memory dial path: try to find a listener registered for this hash, or
/// create a standalone connection. Used as fallback or when real-reticulum
/// feature is disabled.
async fn in_memory_dial(
    dest: &str,
    store: &'static crate::store::HandleStore,
    registry: &'static crate::task::TaskRegistry,
    task_id: u64,
) {
    // Try to find a listener registered for this hash
    match store.get_listener_by_hash(dest).await {
        Some(listener) => {
            // Paired connection: dial-side connection A, listener-side connection B
            let (conn_a, conn_b) = create_pair().await;
            // Push conn_b into the listener's accept queue
            listener.push_connection((*conn_b).clone()).await;
            // Insert conn_a into the store as the dial result
            let handle = store.insert_connection((*conn_a).clone()).await;
            registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
        }
        None => {
            // No matching listener — standalone connection
            let conn = Connection::new();
            let handle = store.insert_connection(conn).await;
            registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
        }
    }
}

/// Listen on a hash, returning a task ID.
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
    
    // Create a pending task
    let task_id = runtime::block_on(async {
        registry.insert_pending().await
    });
    
    // Spawn the async listen operation
    runtime::block_on(async move {
        #[cfg(feature = "real-reticulum")]
        {
            // Real Reticulum path: register a SingleInputDestination with the transport
            // and create a listener that wraps it.
            let listen_hash = dest;
            match crate::transport::get_transport() {
                Some(_) => {
                    // Generate a random identity for this destination
                    let identity = PrivateIdentity::new_from_rand(rand_core::OsRng);
                    
                    // Create the listener first (without destination)
                    let listener = Listener::with_hash(listen_hash.clone());
                    let listener_arc = Arc::new(listener);
                    
                    // Register the destination with the transport and spawn
                    // the background link event subscriber
                    let app_name = "sing-box-reticulum".to_string();
                    let aspect = listen_hash.clone();
                    match crate::transport::register_listener_destination(
                        listener_arc.clone(),
                        identity,
                        app_name,
                        aspect,
                    ).await {
                        Ok(address_hash) => {
                            // Store the address hash on the listener so it can be
                            // retrieved later for dialing.
                            listener_arc.set_destination_hash(address_hash).await;
                            let handle = store.insert_listener((*listener_arc).clone()).await;
                            registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
                        }
                        Err(e) => {
                            registry.complete(
                                task_id,
                                TaskResult::Error {
                                    message: format!("failed to register destination: {}", e),
                                },
                            ).await;
                        }
                    }
                }
                None => {
                    // Transport not initialized — fall back to in-memory mode
                    let listener = Listener::with_hash(listen_hash);
                    let handle = store.insert_listener(listener).await;
                    registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
                }
            }
        }
        
        #[cfg(not(feature = "real-reticulum"))]
        {
            // In-memory path: create a simple listener with the hash
            let listener = Listener::with_hash(dest);
            let handle = store.insert_listener(listener).await;
            registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
        }
    });
    
    task_id as i32
}

/// Close a connection or listener handle.
#[no_mangle]
pub extern "C" fn reticulum_close(handle: u64) {
    let store = global_store();
    runtime::block_on(async move {
        store.remove(handle).await;
    });
}

/// Accept a pending connection from a listener.
/// Returns a task ID. Use reticulum_poll to get the new connection handle.
#[no_mangle]
pub extern "C" fn reticulum_accept(listener_handle: u64) -> i32 {
    let registry = global_registry();
    let store = global_store();

    // Create a pending task
    let task_id = runtime::block_on(async {
        registry.insert_pending().await
    });

    // Spawn the async accept operation
    runtime::block_on(async move {
        match store.get_listener(listener_handle).await {
            Some(listener) => {
                match listener.accept().await {
                    Some(conn) => {
                        let handle = store.insert_connection(conn).await;
                        registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
                    }
                    None => {
                        // No pending connection; treat as error for now
                        registry.complete(task_id, TaskResult::Error { message: "no pending connection".to_string() }).await;
                    }
                }
            }
            None => {
                registry.complete(task_id, TaskResult::Error { message: "invalid listener handle".to_string() }).await;
            }
        }
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
    
    let result = runtime::block_on(async move {
        match store.get_connection(conn_handle).await {
            Some(conn) => conn.write(data_slice).await as i32,
            None => -1,
        }
    });
    result
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
    
    let result = runtime::block_on(async move {
        match store.get_connection(conn_handle).await {
            Some(conn) => conn.read(buffer_slice).await as i32,
            None => -1,
        }
    });
    result
}

/// Allocate a buffer with libc::malloc and copy bytes into it.
/// Returns a pointer suitable for freeing with reticulum_free.
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
pub extern "C" fn reticulum_poll(task_id: i32, result_out: *mut *mut u8, len_out: *mut usize) -> i32 {
    if task_id < 0 {
        return -1;
    }
    let registry = global_registry();
    let result = runtime::block_on(async move {
        registry.get_and_remove(task_id as u64).await
    });
    match result {
        None => 0, // pending
        Some(TaskResult::Done { handle, data: _ }) => {
            // Return the handle as bytes, allocated with libc so reticulum_free works.
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
/// Safe to call on memory allocated by any bridge function (get_hash, reticulum_poll).
#[no_mangle]
pub extern "C" fn reticulum_free(ptr: *mut u8) {
    if !ptr.is_null() {
        unsafe {
            libc::free(ptr as *mut libc::c_void);
        }
    }
}
