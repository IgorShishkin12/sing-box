use std::time::{Duration, Instant};

/// Test that reticulum_poll returns a proper error message with correct length
/// when a task fails (e.g., accept on a non-existent listener handle).
#[test]
fn test_poll_error_returns_message() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(config.as_ptr());
    assert_eq!(ret, 0, "bridge init should succeed");

    // Accept on an invalid listener handle (0) — this should fail.
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_accept(0);
    assert!(task_id >= 0, "accept should return a task ID");

    // Poll in a loop — the spawned task completes asynchronously.
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let mut final_ret = 0i32;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret != 0 {
            final_ret = ret;
            break;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for accept error on invalid handle");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(final_ret, -1, "poll should return -1 for error");
    assert!(len_out > 0, "error message length should be > 0");
    assert!(!result_out.is_null(), "error message pointer should not be null");

    let error_msg = unsafe {
        let slice = std::slice::from_raw_parts(result_out, len_out);
        String::from_utf8_lossy(slice).to_string()
    };
    assert!(!error_msg.is_empty(), "error message should not be empty");
    assert_eq!(error_msg.len(), len_out, "string length should match len_out");

    sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that reticulum_poll returns -1 for an invalid task ID.
#[test]
fn test_poll_invalid_task_id() {
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(-1, &mut result_out, &mut len_out);
    assert_eq!(ret, -1, "poll with negative task ID should return -1");
    assert!(result_out.is_null(), "result_out should be null for invalid task");
    assert_eq!(len_out, 0, "len_out should be 0 for invalid task");
}
