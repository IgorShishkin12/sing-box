use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime;

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

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

/// Dial a destination hash, returning a connection handle.
#[no_mangle]
pub extern "C" fn reticulum_dial(destination_hash: *const c_char) -> u64 {
    if destination_hash.is_null() {
        return 0;
    }
    // For now, just return a unique handle.
    NEXT_HANDLE.fetch_add(1, Ordering::SeqCst)
}

/// Listen on a hash, returning a listener handle.
#[no_mangle]
pub extern "C" fn reticulum_listen(listen_hash: *const c_char) -> u64 {
    if listen_hash.is_null() {
        return 0;
    }
    NEXT_HANDLE.fetch_add(1, Ordering::SeqCst)
}

/// Close a connection or listener handle.
#[no_mangle]
pub extern "C" fn reticulum_close(handle: u64) {
    // No-op for stub.
}

/// Write data to a connection.
#[no_mangle]
pub extern "C" fn reticulum_write(conn_handle: u64, data: *const u8, len: usize) -> i32 {
    if data.is_null() {
        return -1;
    }
    // No-op: pretend write succeeded.
    len as i32
}

/// Read data from a connection.
#[no_mangle]
pub extern "C" fn reticulum_read(conn_handle: u64, buffer: *mut u8, max_len: usize) -> i32 {
    if buffer.is_null() {
        return -1;
    }
    // No data available.
    0
}

/// Poll for completion of a task.
/// Returns 0=pending, 1=done, -1=error.
#[no_mangle]
pub extern "C" fn reticulum_poll(_task_id: i32, _result_out: *mut *mut u8, _len_out: *mut usize) -> i32 {
    // Always pending for stub.
    0
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
