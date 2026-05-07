/// Test that shutdown after init is safe (no panic)
#[test]
fn test_shutdown_after_init() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0);
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that shutdown without init is safe (no panic)
#[test]
fn test_shutdown_without_init() {
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that init → shutdown → init cycle works
#[test]
fn test_init_shutdown_init() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0);
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();

    // Re-init should succeed
    let ret2 = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret2, 0, "re-init should succeed after shutdown");
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that after shutdown, operations still work (auto-reinit)
#[test]
fn test_shutdown_then_dial_still_works() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0);

    // Shutdown
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();

    // Dial should auto-reinit and work
    let dest = std::ffi::CString::new("rln://test-after-shutdown").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_dial(dest.as_ptr());
    assert!(task_id > 0, "dial should work after shutdown+auto-reinit");

    // Poll for completion
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let mut attempts = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret == 1 {
            assert!(!result_out.is_null());
            assert_eq!(len_out, 8);
            let handle_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
            let handle = u64::from_le_bytes(handle_bytes.try_into().unwrap());
            assert!(handle > 0);
            sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            sing_box_reticulum_bridge::c_api::reticulum_close(handle);
            break;
        }
        attempts += 1;
        if attempts > 100 {
            panic!("poll timed out");
        }
    }

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}