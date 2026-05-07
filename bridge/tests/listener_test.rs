/// Test listener happy path: push a connection and accept it
#[test]
fn test_listener_push_and_accept() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0, "bridge init should succeed");

    let listen_hash = std::ffi::CString::new("rln://listener-test").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_listen(listen_hash.as_ptr());
    assert!(task_id > 0, "listen should return a positive task ID");

    // Poll for listener handle
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let mut attempts = 0;
    let mut listener_handle: u64 = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret == 1 {
            assert!(!result_out.is_null());
            assert_eq!(len_out, 8);
            let handle_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
            listener_handle = u64::from_le_bytes(handle_bytes.try_into().unwrap());
            assert!(listener_handle > 0);
            sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            break;
        }
        attempts += 1;
        if attempts > 100 {
            panic!("poll timed out after {} attempts", attempts);
        }
    }

    // Accept on empty queue should fail
    let accept_task_id = sing_box_reticulum_bridge::c_api::reticulum_accept(listener_handle);
    assert!(accept_task_id > 0);

    let mut result_out2: *mut u8 = std::ptr::null_mut();
    let mut len_out2: usize = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(accept_task_id, &mut result_out2, &mut len_out2);
        if ret == -1 {
            // Expected: no pending connection
            assert!(len_out2 > 0, "error message should have content");
            sing_box_reticulum_bridge::c_api::reticulum_free(result_out2);
            break;
        }
        attempts += 1;
        if attempts > 100 {
            panic!("poll for accept timeout");
        }
    }

    sing_box_reticulum_bridge::c_api::reticulum_close(listener_handle);
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test accepting with an invalid listener handle
#[test]
fn test_accept_invalid_handle() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0, "bridge init should succeed");

    // Accept on handle 0 (invalid) — should return a task ID
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_accept(99999);
    assert!(task_id >= 0, "accept should return a task ID");

    // Poll should return error
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    let mut attempts = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret == -1 {
            assert!(len_out > 0, "error message should have content");
            assert!(!result_out.is_null(), "error pointer should not be null");
            sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            break;
        }
        attempts += 1;
        if attempts > 100 {
            panic!("timeout waiting for accept error");
        }
    }

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}