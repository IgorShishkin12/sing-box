use serial_test::serial;
use sing_box_reticulum_bridge::c_api::{emit_log_direct, install_panic_hook, ON_LOG};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

// Global capture buffer for the log callback tests.
static LOG_CAPTURED: AtomicBool = AtomicBool::new(false);
static LOG_CAPTURE: Mutex<Option<(u8, String, String)>> = Mutex::new(None);

extern "C" fn capture_log_cb(
    level: u8,
    target: *const std::os::raw::c_char,
    msg: *const std::os::raw::c_char,
) {
    use std::ffi::CStr;
    let t = unsafe { CStr::from_ptr(target) }
        .to_str()
        .unwrap_or("")
        .to_string();
    let m = unsafe { CStr::from_ptr(msg) }
        .to_str()
        .unwrap_or("")
        .to_string();
    if let Ok(mut g) = LOG_CAPTURE.lock() {
        *g = Some((level, t, m));
    }
    LOG_CAPTURED.store(true, Ordering::SeqCst);
}

#[test]
#[serial]
fn test_emit_log_direct_no_callback_no_panic() {
    ON_LOG.store(0, Ordering::SeqCst);
    // Must not panic even without a registered callback.
    emit_log_direct(1, "test_target", "test message — no callback");
}

#[test]
#[serial]
fn test_emit_log_direct_calls_registered_callback() {
    LOG_CAPTURED.store(false, Ordering::SeqCst);
    *LOG_CAPTURE.lock().unwrap() = None;

    ON_LOG.store(capture_log_cb as *const () as usize, Ordering::SeqCst);
    emit_log_direct(3, "my_target", "hello from emit_log_direct");
    ON_LOG.store(0, Ordering::SeqCst);

    assert!(
        LOG_CAPTURED.load(Ordering::SeqCst),
        "log callback was not called"
    );
    let captured = LOG_CAPTURE.lock().unwrap().clone().unwrap();
    assert_eq!(captured.0, 3);
    assert_eq!(captured.1, "my_target");
    assert_eq!(captured.2, "hello from emit_log_direct");
}

#[test]
#[serial]
fn test_install_panic_hook_routes_panic_without_crash() {
    ON_LOG.store(0, Ordering::SeqCst);
    install_panic_hook();
    let result = std::panic::catch_unwind(|| {
        panic!("test panic for hook verification");
    });
    assert!(result.is_err(), "panic should have been caught");
    // Restore the default hook so subsequent tests behave normally.
    let _ = std::panic::take_hook();
}

#[test]
#[serial]
fn test_install_panic_hook_with_callback_does_not_crash() {
    LOG_CAPTURED.store(false, Ordering::SeqCst);
    *LOG_CAPTURE.lock().unwrap() = None;

    ON_LOG.store(capture_log_cb as *const () as usize, Ordering::SeqCst);
    install_panic_hook();

    let result = std::panic::catch_unwind(|| {
        panic!("hook callback test");
    });
    assert!(result.is_err());

    ON_LOG.store(0, Ordering::SeqCst);
    let _ = std::panic::take_hook();

    // The hook should have fired the callback with level=1 (error).
    assert!(
        LOG_CAPTURED.load(Ordering::SeqCst),
        "panic hook should have called log cb"
    );
    let captured = LOG_CAPTURE.lock().unwrap().clone().unwrap();
    assert_eq!(captured.0, 1, "panic hook should log at error level");
    assert!(
        captured.2.contains("hook callback test"),
        "panic message should appear in log: got {:?}",
        captured.2
    );
}
