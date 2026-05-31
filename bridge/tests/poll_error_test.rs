use sing_box_reticulum_bridge::c_api::*;

/// Test that reticulum_write on an invalid handle returns -1.
#[test]
fn test_write_invalid_handle() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    assert_eq!(ret, 0, "bridge init should succeed");

    let data = b"hello";
    let n = unsafe { reticulum_write(99999, data.as_ptr(), data.len()) };
    assert_eq!(n, -1, "write on invalid handle should return -1");

    unsafe { reticulum_shutdown(); }
}

/// Test that reticulum_close on an invalid handle is a no-op (does not panic).
#[test]
fn test_close_invalid_handle() {
    let config = std::ffi::CString::new("{}").unwrap();
    let _ = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    unsafe { reticulum_close(99999); } // must not panic
    unsafe { reticulum_shutdown(); }
}

/// Test that reticulum_free on null is safe.
#[test]
fn test_free_null() {
    unsafe { reticulum_free(std::ptr::null_mut()); }
}
