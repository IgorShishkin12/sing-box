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
    unsafe {
        reticulum_shutdown();
    }
}

/// Test that shutdown without init is safe (no panic).
#[test]
#[serial]
fn test_shutdown_without_init() {
    unsafe {
        reticulum_shutdown();
    }
}

/// Test that init → shutdown → init cycle works.
#[test]
#[serial]
fn test_init_shutdown_init() {
    init();
    unsafe {
        reticulum_shutdown();
    }
    init();
    unsafe {
        reticulum_shutdown();
    }
}

/// Test that dial after shutdown fires on_connect with conn_id=0.
#[test]
#[serial]
fn test_shutdown_then_dial_returns_error() {
    use std::sync::atomic::{AtomicU64, Ordering};

    static FIRED_CONN_ID: AtomicU64 = AtomicU64::new(u64::MAX);

    extern "C" fn on_connect(_task_id: u64, conn_id: u64) {
        FIRED_CONN_ID.store(conn_id, Ordering::Relaxed);
    }

    FIRED_CONN_ID.store(u64::MAX, Ordering::Relaxed);

    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, Some(on_connect), None, None) };
    assert_eq!(ret, 0);

    unsafe {
        reticulum_shutdown();
    }

    // Dial without transport initialized — should fire callback with conn_id=0.
    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    unsafe {
        reticulum_dial(9001, dest.as_ptr());
    }

    let start = std::time::Instant::now();
    loop {
        if FIRED_CONN_ID.load(Ordering::Relaxed) != u64::MAX {
            break;
        }
        if start.elapsed().as_secs() >= 5 {
            panic!("on_connect did not fire within 5s");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(
        FIRED_CONN_ID.load(Ordering::Relaxed),
        0,
        "dial without transport should yield conn_id=0"
    );
}
