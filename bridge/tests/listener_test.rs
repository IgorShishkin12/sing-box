use std::ffi::CString;
use sing_box_reticulum_bridge::c_api::*;

extern "C" fn noop_accept(_: u64, _: u64, _: *const std::ffi::c_char) {}
extern "C" fn noop_connect(_: u64, _: u64) {}
extern "C" fn noop_data(_: u64, _: *const u8, _: usize) {}
extern "C" fn noop_close(_: u64) {}

fn init() {
    let cfg = CString::new("{}").unwrap();
    let ret = reticulum_init(cfg.as_ptr(), Some(noop_accept), Some(noop_connect), Some(noop_data), Some(noop_close));
    assert_eq!(ret, 0);
}

/// reticulum_listen succeeds with local service registration (no real network needed).
#[test]
fn test_listen_returns_valid_handle() {
    init();

    let name = CString::new("listener-test-no-network").unwrap();
    let id = reticulum_listen(name.as_ptr());
    assert!(id > 0, "reticulum_listen should return a positive listener id, got {}", id);
    reticulum_close(id as u64);

    reticulum_shutdown();
}
