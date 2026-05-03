use sing_box_reticulum_bridge;

#[test]
fn test_dial_poll_handle() {
    // Initialize runtime
    assert_eq!(sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null()), 0);
    
    // Dial a destination
    let dest = std::ffi::CString::new("rln://test-dest").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_dial(dest.as_ptr());
    assert!(task_id > 0, "task_id should be positive, got {}", task_id);
    
    // Poll until done
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    
    // Poll should eventually return 1 (done)
    let mut attempts = 0;
    let mut handle: u64 = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret == 1 {
            // Done - should have a handle
            assert!(!result_out.is_null(), "result_out should not be null");
            assert_eq!(len_out, 8, "handle should be 8 bytes");
            
            // Read the handle
            let handle_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
            handle = u64::from_le_bytes(handle_bytes.try_into().unwrap());
            assert!(handle > 0, "handle should be positive");
            
            // Free the result
            sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            break;
        } else if ret == -1 {
            panic!("poll returned error");
        }
        // ret == 0 means pending, continue polling
        attempts += 1;
        if attempts > 100 {
            panic!("poll timed out after {} attempts", attempts);
        }
    }
    
    // Should be able to write to the connection
    let data = b"hello world";
    let written = unsafe { sing_box_reticulum_bridge::c_api::reticulum_write(handle, data.as_ptr(), data.len()) };
    assert_eq!(written, data.len() as i32);
    
    // Should be able to read from the connection
    let mut buf = [0u8; 11];
    let read = unsafe { sing_box_reticulum_bridge::c_api::reticulum_read(handle, buf.as_mut_ptr(), buf.len()) };
    assert_eq!(read, 11);
    assert_eq!(&buf, data);
    
    // Close the connection
    unsafe { sing_box_reticulum_bridge::c_api::reticulum_close(handle) };
    
    // Shutdown
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

#[test]
fn test_listen_poll_handle() {
    assert_eq!(sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null()), 0);
    
    // Listen on a hash
    let listen_hash = std::ffi::CString::new("rln://listen-hash").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_listen(listen_hash.as_ptr());
    assert!(task_id > 0, "task_id should be positive, got {}", task_id);
    
    // Poll until done
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    
    let mut attempts = 0;
    let mut handle: u64 = 0;
    loop {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        if ret == 1 {
            assert!(!result_out.is_null());
            assert_eq!(len_out, 8);
            
            let handle_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
            handle = u64::from_le_bytes(handle_bytes.try_into().unwrap());
            assert!(handle > 0);
            
            sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
            break;
        } else if ret == -1 {
            panic!("poll returned error");
        }
        attempts += 1;
        if attempts > 100 {
            panic!("poll timed out after {} attempts", attempts);
        }
    }
    
    // Close the listener
    unsafe { sing_box_reticulum_bridge::c_api::reticulum_close(handle) };
    
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

#[test]
fn test_multiple_dials() {
    assert_eq!(sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null()), 0);
    
    let mut handles = vec![];
    
    for i in 0..5 {
        let dest = std::ffi::CString::new(format!("rln://dest-{}", i)).unwrap();
        let task_id = sing_box_reticulum_bridge::c_api::reticulum_dial(dest.as_ptr());
        assert!(task_id > 0);
        
        let mut result_out: *mut u8 = std::ptr::null_mut();
        let mut len_out: usize = 0;
        
        loop {
            let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
            if ret == 1 {
                let handle_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                let handle = u64::from_le_bytes(handle_bytes.try_into().unwrap());
                handles.push(handle);
                sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
                break;
            }
        }
    }
    
    // All handles should be unique
    for i in 0..handles.len() {
        for j in i+1..handles.len() {
            assert_ne!(handles[i], handles[j], "handles should be unique");
        }
    }
    
    for handle in handles {
        unsafe { sing_box_reticulum_bridge::c_api::reticulum_close(handle) };
    }
    
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}
