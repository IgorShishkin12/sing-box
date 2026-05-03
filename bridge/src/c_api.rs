use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use crate::runtime;
use crate::task::{TaskResult, global_registry};

/// Initialize the reticulum bridge with a JSON config string.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_init(config_json: *const c_char) -> i32 {
    if config_json.is_null() {
        return -1;
    }
    let c_str = unsafe { CStr::from_ptr(config_json) };
    let _config_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    // TODO: parse config and store globally
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
    let _dest = unsafe { CStr::from_ptr(destination_hash) }.to_string_lossy().into_owned();
    // For now, create a stub connection and return task
    let registry = global_registry();
    let task_id = runtime::block_on(async {
        registry.insert(TaskResult::Done { handle: 1, data: vec![] }).await
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
    let _dest = unsafe { CStr::from_ptr(listen_hash) }.to_string_lossy().into_owned();
    let registry = global_registry();
    let task_id = runtime::block_on(async {
        registry.insert(TaskResult::Done { handle: 1, data: vec![] }).await
    });
    task_id as i32
}

/// Close a connection or listener handle.
#[no_mangle]
pub extern "C" fn reticulum_close(_handle: u64) {
    // No-op for stub.
}

/// Write data to a connection.
/// Returns number of bytes written, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_write(_conn_handle: u64, data: *const u8, len: usize) -> i32 {
    if data.is_null() || len == 0 {
        return -1;
    }
    // For stub, just return len
    len as i32
}

/// Read data from a connection.
/// Returns number of bytes read, or -1 on error.
#[no_mangle]
pub extern "C" fn reticulum_read(_conn_handle: u64, buffer: *mut u8, max_len: usize) -> i32 {
    if buffer.is_null() || max_len == 0 {
        return -1;
    }
    // No data available for stub.
    0
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
            let boxed_slice = msg_bytes.into_boxed_slice();
            let ptr = Box::into_raw(boxed_slice) as *mut u8;
            unsafe {
                if !result_out.is_null() {
                    *result_out = ptr;
                }
                if !len_out.is_null() {
                    *len_out = 0;
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
            let _ = Vec::from_raw_parts(ptr, 0, 0);
        }
    }
}
