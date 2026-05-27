use serial_test::serial;
use std::ffi::CString;

extern "C" fn noop_accept(_: u64, _: u64, _: *const std::ffi::c_char) {}
extern "C" fn noop_connect(_: u64, _: u64) {}
extern "C" fn noop_data(_: u64, _: *const u8, _: usize) {}
extern "C" fn noop_close(_: u64) {}

fn bridge_init() {
    let config = CString::new("{}").unwrap();
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(
        config.as_ptr(),
        Some(noop_accept),
        Some(noop_connect),
        Some(noop_data),
        Some(noop_close),
    );
    assert_eq!(ret, 0);
}

fn bridge_shutdown() {
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// reticulum_resolve_name with null pointer must not crash.
#[test]
fn test_resolve_name_null() {
    let result = sing_box_reticulum_bridge::c_api::reticulum_resolve_name(std::ptr::null());
    assert!(result.is_null(), "null input should return null");
}

/// reticulum_resolve_name times out cleanly when no transport peer exists.
#[test]
#[serial]
fn test_resolve_name_no_transport_returns_null() {
    bridge_init();

    // Without a real network peer, resolve exhausts all retries and must return NULL.
    let name = CString::new("nonexistent-service-x").unwrap();
    let result = sing_box_reticulum_bridge::c_api::reticulum_resolve_name(name.as_ptr());
    assert!(result.is_null(), "unresolvable name should return null after timeout");

    bridge_shutdown();
}

/// reticulum_free on null is a no-op.
#[test]
fn test_free_null_is_noop() {
    sing_box_reticulum_bridge::c_api::reticulum_free(std::ptr::null_mut());
}
