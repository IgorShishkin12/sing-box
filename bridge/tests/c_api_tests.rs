use std::ffi::{CStr, CString};
use std::os::raw::c_char;

extern "C" {
    fn reticulum_init(config_json: *const c_char) -> i32;
    fn reticulum_shutdown();
    fn reticulum_dial(destination_hash: *const c_char) -> u64;
    fn reticulum_listen(listen_hash: *const c_char) -> u64;
    fn reticulum_close(handle: u64);
    fn reticulum_write(conn_handle: u64, data: *const u8, len: usize) -> i32;
    fn reticulum_read(conn_handle: u64, buffer: *mut u8, max_len: usize) -> i32;
    fn reticulum_poll(task_id: i32, result_out: *mut *mut u8, len_out: *mut usize) -> i32;
    fn reticulum_free(ptr: *mut u8);
}

#[test]
fn test_reticulum_init_returns_zero() {
    let config = CString::new(r#"{}"#).unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr()) };
    assert_eq!(ret, 0, "init should return 0 on success");
    unsafe { reticulum_shutdown(); }
}

#[test]
fn test_reticulum_dial_returns_nonzero_handle() {
    let config = CString::new(r#"{}"#).unwrap();
    unsafe { reticulum_init(config.as_ptr()); }
    let hash = CString::new("test-destination").unwrap();
    let handle = unsafe { reticulum_dial(hash.as_ptr()) };
    assert!(handle > 0, "dial should return a positive handle");
    unsafe { reticulum_close(handle); }
    unsafe { reticulum_shutdown(); }
}

#[test]
fn test_reticulum_poll_returns_pending_on_new_task() {
    let config = CString::new(r#"{}"#).unwrap();
    unsafe { reticulum_init(config.as_ptr()); }
    let hash = CString::new("test-listen").unwrap();
    let handle = unsafe { reticulum_listen(hash.as_ptr()) };
    assert!(handle > 0);
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let ret = unsafe { reticulum_poll(1, &mut result_out, &mut len_out) };
    assert_eq!(ret, 0, "poll should return 0 (pending) for new task");
    unsafe { reticulum_close(handle); }
    unsafe { reticulum_shutdown(); }
}

#[test]
fn test_reticulum_shutdown_is_idempotent() {
    let config = CString::new(r#"{}"#).unwrap();
    unsafe { reticulum_init(config.as_ptr()); }
    unsafe { reticulum_shutdown(); }
    unsafe { reticulum_shutdown(); } // should not crash
}
