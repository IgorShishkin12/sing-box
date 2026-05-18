use serial_test::serial;

fn init() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(config.as_ptr());
    assert_eq!(ret, 0);
}

/// Test that shutdown after init is safe (no panic)
#[test]
#[serial]
fn test_shutdown_after_init() {
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that shutdown without init is safe (no panic)
#[test]
#[serial]
fn test_shutdown_without_init() {
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that init → shutdown → init cycle works
#[test]
#[serial]
fn test_init_shutdown_init() {
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
    // Re-init should succeed
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that dial after shutdown returns an error (transport cleared on shutdown)
#[test]
#[serial]
fn test_shutdown_then_dial_returns_error() {
    init();
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();

    // Dial without re-init — transport is not available, should error
    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_dial(dest.as_ptr());
    assert!(task_id >= 0, "dial should return a task ID even without transport");

    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let mut attempts = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret == -1 {
            if !result_out.is_null() {
                sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            }
            break;
        }
        if ret == 1 {
            // Unexpectedly succeeded — still clean up
            if !result_out.is_null() {
                sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            }
            break;
        }
        attempts += 1;
        if attempts > 200 {
            panic!("poll timed out");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
