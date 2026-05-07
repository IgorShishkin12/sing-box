use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use crate::config;
use crate::connection::Connection;
use crate::listener::Listener;
use crate::runtime;
use crate::store::global_store;
use crate::task::{TaskResult, global_registry};

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
    runtime::init_runtime()
}


/// Get the destination hash for a given name.
#[no_mangle]
pub extern "C" fn get_hash(hash: *mut *mut c_char, name: *const c_char) {
    if hash.is_null() || name.is_null() {
        return;
    }
    let name_str = unsafe { CStr::from_ptr(name) }.to_string_lossy().into_owned();
    // TODO: implement actual lookup
    let fake_hash = format!("rln://{}", name_str);
    let c_hash = CString::new(fake_hash).unwrap();
    unsafe {
        *hash = c_hash.into_raw();
    }
}

/// Shutdown the bridge and release resources.
#[no_mangle]
pub extern "C" fn reticulum_shutdown() {
    runtime::shutdown();
}

/// Dial a destination hash, returning a task ID.
/// Use reticulum_poll to check for completion and get the connection handle.
#[no_mangle]
pub extern "C" fn reticulum_dial(destination_hash: *const c_char) -> i32 {
    if destination_hash.is_null() {
        return -1;
    }
    let _dest = match unsafe { CStr::from_ptr(destination_hash) }.to_str() {
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
        // Create a new connection (in-memory for now, will be replaced with rns-transport)
        let conn = Connection::new();
        let handle = store.insert_connection(conn).await;
        
        // Complete the task with the handle
        registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
    });
    
    task_id as i32
}

/// Listen on a hash, returning a task ID.
/// Use reticulum_poll to check for completion and get the listener handle.
#[no_mangle]
pub extern "C" fn reticulum_listen(listen_hash: *const c_char) -> i32 {
    if listen_hash.is_null() {
        return -1;
    }
    let _dest = match unsafe { CStr::from_ptr(listen_hash) }.to_str() {
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
        // Create a new listener (in-memory for now, will be replaced with rns-transport)
        let listener = Listener::new();
        let handle = store.insert_listener(listener).await;
        
        // Complete the task with the handle
        registry.complete(task_id, TaskResult::Done { handle, data: vec![] }).await;
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
            // Return the handle as bytes
            let handle_bytes = handle.to_le_bytes().to_vec();
            let len = handle_bytes.len();
            let boxed_slice = handle_bytes.into_boxed_slice();
            let ptr = Box::into_raw(boxed_slice) as *mut u8;
            unsafe {
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
            let boxed_slice = msg_bytes.into_boxed_slice();
            let ptr = Box::into_raw(boxed_slice) as *mut u8;
            unsafe {
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
            // Box<[u8]> allocated via Box::into_raw in reticulum_poll.
            // We need to reconstruct the Box to drop it properly.
            // Since we don't have the length, use libc::free which is
            // compatible with the system allocator used by Box.
            libc::free(ptr as *mut libc::c_void);
        }
    }
}
