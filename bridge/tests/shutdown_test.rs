use serial_test::serial;
use sing_box_reticulum_bridge::c_api::*;

fn init() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    assert_eq!(ret, 0);
}

/// Test that shutdown after init is safe (no panic).
#[test]
#[serial]
fn test_shutdown_after_init() {
    init();
    reticulum_shutdown();
}

/// Test that shutdown without init is safe (no panic).
#[test]
#[serial]
fn test_shutdown_without_init() {
    reticulum_shutdown();
}

/// Test that init → shutdown → init cycle works.
#[test]
#[serial]
fn test_init_shutdown_init() {
    init();
    reticulum_shutdown();
    init();
    reticulum_shutdown();
}

/// Test that transport identity hash is available after re-init, and is different
/// each time (ephemeral identity — no identity_name/key in "{}").
#[test]
#[serial]
fn test_transport_reinit_creates_new_identity() {
    init();
    let h1 = reticulum_get_transport_hash();
    assert!(!h1.is_null(), "transport hash must be non-null after first init");

    reticulum_shutdown();
    init();

    let h2 = reticulum_get_transport_hash();
    assert!(!h2.is_null(), "transport hash must be non-null after second init");

    let s1 = unsafe { std::ffi::CStr::from_ptr(h1).to_str().unwrap().to_string() };
    let s2 = unsafe { std::ffi::CStr::from_ptr(h2).to_str().unwrap().to_string() };
    assert_ne!(s1, s2, "ephemeral identity must differ across reinit");

    unsafe {
        reticulum_free(h1 as *mut u8);
        reticulum_free(h2 as *mut u8);
    }
    reticulum_shutdown();
}

/// Test that callbacks are cleared on shutdown so stale tasks can't fire them.
#[test]
#[serial]
fn test_callbacks_cleared_after_shutdown() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static FIRED: AtomicU64 = AtomicU64::new(0);

    extern "C" fn on_close(conn_id: u64) {
        FIRED.store(conn_id, Ordering::SeqCst);
    }

    FIRED.store(0, Ordering::SeqCst);

    let config = std::ffi::CString::new("{}").unwrap();
    unsafe { reticulum_init(config.as_ptr(), None, None, None, Some(on_close)) };

    reticulum_shutdown();

    // After shutdown the on_close pointer should be zero — calling it would be UaF.
    // We verify indirectly: call reticulum_close on a bogus handle.
    // If the callback were still set it would fire; since it's cleared it must not.
    reticulum_close(9999);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(FIRED.load(Ordering::SeqCst), 0, "on_close must not fire after shutdown");
}

/// Test that dial after shutdown is a silent no-op: callbacks are cleared on
/// shutdown so no function pointer is invoked (which would be use-after-free).
#[test]
#[serial]
fn test_dial_after_shutdown_is_noop() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static FIRED: AtomicBool = AtomicBool::new(false);

    extern "C" fn on_connect(_task_id: u64, _conn_id: u64) {
        FIRED.store(true, Ordering::SeqCst);
    }

    FIRED.store(false, Ordering::SeqCst);

    let config = std::ffi::CString::new("{}").unwrap();
    unsafe { reticulum_init(config.as_ptr(), None, Some(on_connect), None, None) };
    reticulum_shutdown();

    // All callbacks are zeroed on shutdown. Dial must not invoke on_connect.
    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    unsafe { reticulum_dial(9001, dest.as_ptr()) };
    std::thread::sleep(std::time::Duration::from_millis(20));

    assert!(!FIRED.load(Ordering::SeqCst), "on_connect must not fire after shutdown");
}
