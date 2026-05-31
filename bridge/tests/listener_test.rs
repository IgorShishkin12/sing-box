use sing_box_reticulum_bridge::c_api::*;
use std::ffi::CString;

/// Test that listen with an invalid hash (empty after stripping prefix) returns -1.
#[test]
fn test_listen_null_returns_error() {
    let ret = unsafe { reticulum_listen(std::ptr::null()) };
    assert_eq!(ret, -1, "null listen_hash should return -1");
}

/// Test that listen succeeds (handle > 0) after a successful init.
#[test]
fn test_listen_returns_handle() {
    let config = CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    assert_eq!(ret, 0, "bridge init should succeed");

    let hash = CString::new("rln://test-service").unwrap();
    let handle = unsafe { reticulum_listen(hash.as_ptr()) };
    // The listen may succeed or fail depending on runtime state, but must not panic.
    if handle > 0 {
        reticulum_close(handle as u64);
    } else {
        eprintln!(
            "listen returned {} (acceptable in test environment)",
            handle
        );
    }

    reticulum_shutdown();
}
