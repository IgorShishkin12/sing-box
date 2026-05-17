use std::ffi::CString;
use std::ptr;
use std::time::Duration;

use sing_box_reticulum_bridge::c_api::*;

fn poll_task(task_id: i32, timeout: Duration) -> Result<u64, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err("poll timeout".to_string());
        }
        let mut result_out: *mut u8 = ptr::null_mut();
        let mut len_out: usize = 0;
        let status = reticulum_poll(task_id, &mut result_out, &mut len_out);
        match status {
            1 => {
                let bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                let handle = u64::from_le_bytes(bytes.try_into().unwrap());
                reticulum_free(result_out);
                return Ok(handle);
            }
            -1 => {
                let msg = if !result_out.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                    let s = String::from_utf8_lossy(bytes).to_string();
                    reticulum_free(result_out);
                    s
                } else {
                    "unknown error".to_string()
                };
                return Err(msg);
            }
            _ => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

/// Test accepting with an invalid listener handle returns an error quickly.
#[test]
fn test_accept_invalid_handle() {
    let config = CString::new("{}").unwrap();
    let ret = reticulum_init(config.as_ptr());
    assert_eq!(ret, 0, "bridge init should succeed");

    // Handle 99999 doesn't exist — the spawned task should fail immediately.
    let task_id = reticulum_accept(99999);
    assert!(task_id >= 0, "accept should return a task ID");

    let result = poll_task(task_id, Duration::from_secs(2));
    assert!(result.is_err(), "accept on invalid handle should fail");

    reticulum_shutdown();
}
