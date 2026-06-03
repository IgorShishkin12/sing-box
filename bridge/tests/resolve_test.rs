use serial_test::serial;
use sing_box_reticulum_bridge::c_api::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static RESOLVE_FIRED: AtomicBool = AtomicBool::new(false);
static RESOLVE_TASK: AtomicU64 = AtomicU64::new(u64::MAX);
static RESOLVE_NULL: AtomicBool = AtomicBool::new(false);

extern "C" fn on_resolve(task_id: u64, hash: *const std::os::raw::c_char) {
    RESOLVE_TASK.store(task_id, Ordering::SeqCst);
    RESOLVE_NULL.store(hash.is_null(), Ordering::SeqCst);
    if !hash.is_null() {
        unsafe { reticulum_free(hash as *mut u8) };
    }
    RESOLVE_FIRED.store(true, Ordering::SeqCst);
}

fn reset() {
    RESOLVE_FIRED.store(false, Ordering::SeqCst);
    RESOLVE_TASK.store(u64::MAX, Ordering::SeqCst);
    RESOLVE_NULL.store(false, Ordering::SeqCst);
}

fn wait_resolve(task_id: u64, max_ms: u64) {
    let start = std::time::Instant::now();
    loop {
        if RESOLVE_FIRED.load(Ordering::SeqCst) && RESOLVE_TASK.load(Ordering::SeqCst) == task_id
        {
            return;
        }
        if start.elapsed().as_millis() as u64 >= max_ms {
            panic!("on_resolve did not fire for task {} within {}ms", task_id, max_ms);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// Null name must fire on_resolve with null immediately (no network wait).
#[test]
#[serial]
fn test_resolve_null_name_fires_null() {
    reset();
    reticulum_set_resolve_callback(Some(on_resolve));
    unsafe { reticulum_resolve_name(42, std::ptr::null()) };
    wait_resolve(42, 500);
    assert!(RESOLVE_NULL.load(Ordering::SeqCst), "null name must resolve to null");
    // No runtime was started — no shutdown needed.
}

/// Empty name must fire on_resolve with null immediately.
#[test]
#[serial]
fn test_resolve_empty_name_fires_null() {
    reset();
    let config = std::ffi::CString::new("{}").unwrap();
    unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    reticulum_set_resolve_callback(Some(on_resolve));

    let empty = std::ffi::CString::new("").unwrap();
    unsafe { reticulum_resolve_name(43, empty.as_ptr()) };
    wait_resolve(43, 500);
    assert!(RESOLVE_NULL.load(Ordering::SeqCst), "empty name must resolve to null");

    reticulum_shutdown();
}

/// Resolve after shutdown fires nothing (callbacks are cleared).
#[test]
#[serial]
fn test_resolve_after_shutdown_is_noop() {
    reset();
    let config = std::ffi::CString::new("{}").unwrap();
    unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    reticulum_set_resolve_callback(Some(on_resolve));
    reticulum_shutdown();

    let name = std::ffi::CString::new("some-service").unwrap();
    unsafe { reticulum_resolve_name(44, name.as_ptr()) };
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        !RESOLVE_FIRED.load(Ordering::SeqCst),
        "on_resolve must not fire after shutdown (callback was cleared)"
    );
}
