use serial_test::serial;
use sing_box_reticulum_bridge::c_api::*;
use std::sync::atomic::{AtomicI32, Ordering};

static WRITE_RESULT: AtomicI32 = AtomicI32::new(i32::MAX);

extern "C" fn capture_write(_task_id: u64, bytes: i32) {
    WRITE_RESULT.store(bytes, Ordering::Relaxed);
}

fn wait_for_write(max_ms: u64) -> i32 {
    let start = std::time::Instant::now();
    loop {
        let v = WRITE_RESULT.load(Ordering::Relaxed);
        if v != i32::MAX {
            return v;
        }
        if start.elapsed().as_millis() as u64 >= max_ms {
            panic!("on_write did not fire within {}ms", max_ms);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// Test that reticulum_write on an invalid handle fires on_write with -1.
#[test]
#[serial]
fn test_write_invalid_handle() {
    WRITE_RESULT.store(i32::MAX, Ordering::Relaxed);
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    assert_eq!(ret, 0, "bridge init should succeed");

    reticulum_set_write_callback(Some(capture_write));

    let data = b"hello";
    let task_id: u64 = 8001;
    unsafe { reticulum_write(task_id, 99999, data.as_ptr(), data.len()) };

    let n = wait_for_write(5_000);
    assert_eq!(
        n, -1,
        "write on invalid handle should fire on_write with -1"
    );

    reticulum_shutdown();
}

/// Test that reticulum_close on an invalid handle is a no-op (does not panic).
#[test]
#[serial]
fn test_close_invalid_handle() {
    let config = std::ffi::CString::new("{}").unwrap();
    let _ = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    reticulum_close(99999); // must not panic
    reticulum_shutdown();
}

/// Test that reticulum_free on null is safe.
#[test]
fn test_free_null() {
    reticulum_free(std::ptr::null_mut());
}
