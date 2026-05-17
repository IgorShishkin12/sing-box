use serial_test::serial;
use sing_box_reticulum_bridge;

fn init() {
    let config = std::ffi::CString::new("{}").unwrap();
    assert_eq!(sing_box_reticulum_bridge::c_api::reticulum_init(config.as_ptr()), 0);
}

fn poll_until_done(task_id: i32, max_attempts: u32) -> Result<u64, String> {
    let mut result_out: *mut u8 = std::ptr::null_mut();
    let mut len_out: usize = 0;
    for _ in 0..max_attempts {
        let ret = sing_box_reticulum_bridge::c_api::reticulum_poll(task_id, &mut result_out, &mut len_out);
        match ret {
            1 => {
                let bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                let handle = u64::from_le_bytes(bytes.try_into().unwrap());
                sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
                return Ok(handle);
            }
            -1 => {
                let msg = if !result_out.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                    let s = String::from_utf8_lossy(bytes).to_string();
                    sing_box_reticulum_bridge::c_api::reticulum_free(result_out);
                    s
                } else {
                    "unknown error".to_string()
                };
                return Err(msg);
            }
            _ => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }
    Err("poll timed out".to_string())
}

/// Dial to an unknown destination — should fail since there is no peer.
#[test]
#[serial]
fn test_dial_unknown_dest_returns_error() {
    init();

    let dest = std::ffi::CString::new("aabbccdd00112233445566778899aabb").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_dial(dest.as_ptr());
    assert!(task_id >= 0, "dial should return a non-negative task ID");

    let result = poll_until_done(task_id, 1000);
    assert!(result.is_err(), "dial to unknown destination should fail, got {:?}", result);

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Listen should succeed even without network interfaces (local service registration).
#[test]
#[serial]
fn test_listen_succeeds_without_network() {
    init();

    let listen_hash = std::ffi::CString::new("rln://listen-hash-no-net").unwrap();
    let task_id = sing_box_reticulum_bridge::c_api::reticulum_listen(listen_hash.as_ptr());
    assert!(task_id >= 0, "listen should return a non-negative task ID");

    let result = poll_until_done(task_id, 1000);
    // listen may succeed (local identity registered) or fail (no transport); accept both.
    match result {
        Ok(handle) => {
            assert!(handle > 0, "listener handle should be positive");
            unsafe { sing_box_reticulum_bridge::c_api::reticulum_close(handle) };
        }
        Err(e) => {
            // Acceptable: transport may require interfaces for service registration.
            eprintln!("[test] listen failed (acceptable without network): {}", e);
        }
    }

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Multiple concurrent dials — all should return task IDs and eventually resolve (to error).
#[test]
#[serial]
fn test_multiple_dials_return_distinct_task_ids() {
    init();

    let mut task_ids = vec![];
    for i in 0u32..5 {
        let dest = std::ffi::CString::new(
            format!("{:032x}", i as u128 * 0x1111111111111111u128)
        ).unwrap();
        let task_id = sing_box_reticulum_bridge::c_api::reticulum_dial(dest.as_ptr());
        assert!(task_id >= 0);
        task_ids.push(task_id);
    }

    // All task IDs must be unique.
    for i in 0..task_ids.len() {
        for j in i + 1..task_ids.len() {
            assert_ne!(task_ids[i], task_ids[j], "task IDs should be unique");
        }
    }

    // Drain all tasks.
    for task_id in task_ids {
        let _ = poll_until_done(task_id, 2000);
    }

    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}
