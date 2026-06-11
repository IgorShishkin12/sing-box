use serial_test::serial;
use sing_box_reticulum_bridge::c_api::{
    emit_log_direct, install_panic_hook, reticulum_init, reticulum_set_log_callback,
    reticulum_shutdown,
};
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::Mutex;

static CAPTURED: Mutex<Vec<String>> = Mutex::new(Vec::new());

extern "C" fn capture_log(_level: u8, _target: *const c_char, message: *const c_char) {
    let msg = unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    CAPTURED.lock().unwrap().push(msg);
}

fn captured_messages() -> Vec<String> {
    CAPTURED.lock().unwrap().clone()
}

fn clear_captured() {
    CAPTURED.lock().unwrap().clear();
}

/// Panic hook routes panic message + location through the log callback.
#[test]
#[serial]
fn test_panic_hook_formats_and_routes() {
    clear_captured();
    reticulum_set_log_callback(Some(capture_log));
    install_panic_hook();

    // Trigger a panic — the hook fires, then catch_unwind recovers.
    let _ = std::panic::catch_unwind(|| panic!("sentinel panic payload"));

    let msgs = captured_messages();
    assert!(
        msgs.iter().any(|m| m.contains("=== RUST PANIC ===")),
        "expected panic header in log output, got: {msgs:?}"
    );
    assert!(
        msgs.iter().any(|m| m.contains("sentinel panic payload")),
        "expected panic message in log output, got: {msgs:?}"
    );
    assert!(
        msgs.iter().any(|m| m.contains("location:")),
        "expected location line in log output, got: {msgs:?}"
    );
    assert!(
        msgs.iter().any(|m| m.contains("==================") || m.contains("backtrace:")),
        "expected closing line or backtrace in log output, got: {msgs:?}"
    );

    reticulum_shutdown();
    clear_captured();
}

/// emit_log_direct sends a message at the given level through ON_LOG.
#[test]
#[serial]
fn test_emit_log_direct() {
    clear_captured();
    reticulum_set_log_callback(Some(capture_log));

    emit_log_direct(1, "test-target", "hello direct");

    let msgs = captured_messages();
    assert!(
        msgs.iter().any(|m| m.contains("hello direct")),
        "expected direct message, got: {msgs:?}"
    );

    reticulum_shutdown();
    clear_captured();
}

/// reticulum_init installs the panic hook automatically.
#[test]
#[serial]
fn test_init_installs_panic_hook() {
    clear_captured();
    reticulum_set_log_callback(Some(capture_log));

    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe { reticulum_init(config.as_ptr(), None, None, None, None) };
    assert_eq!(ret, 0, "reticulum_init should succeed with empty config");

    let _ = std::panic::catch_unwind(|| panic!("init-hook-test"));

    let msgs = captured_messages();
    assert!(
        msgs.iter().any(|m| m.contains("=== RUST PANIC ===")),
        "panic hook should be active after reticulum_init, got: {msgs:?}"
    );

    reticulum_shutdown();
    clear_captured();
}
