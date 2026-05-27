use serial_test::serial;
use sing_box_reticulum_bridge;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Shared test callbacks
// ---------------------------------------------------------------------------

static CONNECT_TASK_ID: AtomicU64 = AtomicU64::new(0);
static CONNECT_CONN_ID: AtomicU64 = AtomicU64::new(u64::MAX); // MAX = not yet set

extern "C" fn on_accept(_listener_id: u64, _conn_id: u64, _peer_hash: *const std::ffi::c_char) {}
extern "C" fn on_connect(task_id: u64, conn_id: u64) {
    CONNECT_TASK_ID.store(task_id, Ordering::Release);
    CONNECT_CONN_ID.store(conn_id, Ordering::Release);
}
extern "C" fn on_data(_conn_id: u64, _data: *const u8, _len: usize) {}
extern "C" fn on_close(_conn_id: u64) {}

fn init_with_callbacks() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(
        config.as_ptr(),
        Some(on_accept),
        Some(on_connect),
        Some(on_data),
        Some(on_close),
    );
    assert_eq!(ret, 0, "bridge init should succeed");
}

fn wait_for_connect(task_id: u64, timeout: Duration) -> Option<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        if CONNECT_TASK_ID.load(Ordering::Acquire) == task_id {
            let conn_id = CONNECT_CONN_ID.load(Ordering::Acquire);
            return Some(conn_id);
        }
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Dial to an unknown destination — on_connect should fire with conn_id == 0.
#[test]
#[serial]
fn test_dial_unknown_dest_fires_callback_with_zero() {
    CONNECT_TASK_ID.store(0, Ordering::Release);
    CONNECT_CONN_ID.store(u64::MAX, Ordering::Release);
    init_with_callbacks();

    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    let task_id: u64 = 42;
    sing_box_reticulum_bridge::c_api::reticulum_dial(task_id, dest.as_ptr());

    let result = wait_for_connect(task_id, Duration::from_secs(35));
    assert!(result.is_some(), "on_connect callback should fire within timeout");
    assert_eq!(result.unwrap(), 0, "conn_id should be 0 for failed dial");

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// reticulum_listen succeeds with local service registration (no network needed).
#[test]
#[serial]
fn test_listen_returns_listener_id() {
    init_with_callbacks();

    let name = std::ffi::CString::new("test-listen-no-net").unwrap();
    let id = sing_box_reticulum_bridge::c_api::reticulum_listen(name.as_ptr());
    assert!(id > 0, "reticulum_listen should return positive id, got {}", id);
    sing_box_reticulum_bridge::c_api::reticulum_close(id as u64);

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Multiple concurrent dials each get a unique task_id and each fire on_connect.
#[test]
#[serial]
fn test_multiple_dials_all_fire_callback() {
    use std::sync::{Arc, Mutex};

    let results: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let results_cb = results.clone();

    // We can't capture a closure in an extern "C" fn, so use a global for this test.
    static MULTI_RESULTS: std::sync::Mutex<Vec<(u64, u64)>> = std::sync::Mutex::new(Vec::new());

    extern "C" fn multi_on_connect(task_id: u64, conn_id: u64) {
        MULTI_RESULTS.lock().unwrap().push((task_id, conn_id));
    }

    let config = std::ffi::CString::new("{}").unwrap();
    let _ = MULTI_RESULTS.lock().map(|mut v| v.clear());
    let _ = sing_box_reticulum_bridge::c_api::reticulum_init(
        config.as_ptr(),
        Some(on_accept),
        Some(multi_on_connect),
        Some(on_data),
        Some(on_close),
    );

    let task_ids: Vec<u64> = (1..=3).collect();
    for &tid in &task_ids {
        let dest = std::ffi::CString::new(
            format!("{:032x}", tid as u128 * 0x1111111111111111u128)
        ).unwrap();
        sing_box_reticulum_bridge::c_api::reticulum_dial(tid, dest.as_ptr());
    }

    // Wait for all callbacks to fire.
    let deadline = Instant::now() + Duration::from_secs(35 * task_ids.len() as u64 + 5);
    loop {
        let count = MULTI_RESULTS.lock().unwrap().len();
        if count >= task_ids.len() { break; }
        if Instant::now() > deadline {
            panic!("only {} of {} on_connect callbacks fired", count, task_ids.len());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Suppress unused variable warning
    let _ = results_cb;

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// reticulum_write on an unknown handle returns -1.
#[test]
fn test_write_on_invalid_handle_returns_minus_one() {
    let data = b"hello";
    let ret = sing_box_reticulum_bridge::c_api::reticulum_write(
        99999, data.as_ptr(), data.len());
    assert_eq!(ret, -1, "write on unknown handle should return -1");
}

/// reticulum_write with null data pointer returns -1.
#[test]
fn test_write_null_data_returns_minus_one() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_write(0, std::ptr::null(), 0);
    assert_eq!(ret, -1, "write with null data should return -1");
}
