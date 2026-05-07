/// Test that reticulum_poll returns a proper error message with correct length
/// when a task fails (e.g., accept on a non-existent listener handle).
#[test]
fn test_poll_error_returns_message() {
    // Initialize bridge
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0, "bridge init should succeed");

    // Accept on an invalid listener handle (0) — this should fail
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_accept(0);
    assert!(task_id >= 0, "accept should return a task ID");

    // Poll for the result — should return -1 with an error message
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
    assert_eq!(ret, -1, "poll should return -1 for error");

    // The error message should be non-empty
    assert!(len_out > 0, "error message length should be > 0");
    assert!(!result_out.is_null(), "error message pointer should not be null");

    // Read the error message
    let error_msg = unsafe {
        let slice = std::slice::from_raw_parts(result_out, len_out);
        String::from_utf8_lossy(slice).to_string()
    };
    assert!(!error_msg.is_empty(), "error message should not be empty");
    assert_eq!(error_msg.len(), len_out, "string length should match len_out");

    // Free the error message
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
    // No allocation should have been made
    assert!(result_out.is_null(), "result_out should be null for invalid task");
    assert_eq!(len_out, 0, "len_out should be 0 for invalid task");
}