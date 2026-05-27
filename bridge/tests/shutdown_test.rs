use serial_test::serial;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

extern "C" fn noop_accept(_: u64, _: u64, _: *const std::ffi::c_char) {}
extern "C" fn noop_close(_: u64) {}
extern "C" fn noop_data(_: u64, _: *const u8, _: usize) {}

static SHUTDOWN_CONN_ID: AtomicU64 = AtomicU64::new(u64::MAX);
static SHUTDOWN_TASK_ID: AtomicU64 = AtomicU64::new(u64::MAX);

extern "C" fn capture_connect(task_id: u64, conn_id: u64) {
    SHUTDOWN_TASK_ID.store(task_id, Ordering::Release);
    SHUTDOWN_CONN_ID.store(conn_id, Ordering::Release);
}

fn init() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(
        config.as_ptr(),
        Some(noop_accept),
        Some(capture_connect),
        Some(noop_data),
        Some(noop_close),
    );
    assert_eq!(ret, 0);
}

/// Shutdown after init is safe.
#[test]
#[serial]
fn test_shutdown_after_init() {
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Shutdown without init is safe.
#[test]
#[serial]
fn test_shutdown_without_init() {
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Init → shutdown → init cycle works.
#[test]
#[serial]
fn test_init_shutdown_init() {
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Dial after shutdown fires on_connect with conn_id == 0.
#[test]
#[serial]
fn test_shutdown_then_dial_fires_error_callback() {
    SHUTDOWN_TASK_ID.store(u64::MAX, Ordering::Release);
    SHUTDOWN_CONN_ID.store(u64::MAX, Ordering::Release);

    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();

    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    let task_id: u64 = 77;
    sing_box_reticulum_bridge::c_api::reticulum_dial(task_id, dest.as_ptr());

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if SHUTDOWN_TASK_ID.load(Ordering::Acquire) == task_id {
            assert_eq!(
                SHUTDOWN_CONN_ID.load(Ordering::Acquire),
                0,
                "conn_id should be 0 when transport is down"
            );
            break;
        }
        if Instant::now() > deadline {
            panic!("on_connect callback not fired within 5 s");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
