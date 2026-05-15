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

/// Test listener + in-memory dial: accept waits and succeeds when a connection arrives.
#[test]
fn test_listener_push_and_accept() {
    let ret = reticulum_init(ptr::null());
    assert_eq!(ret, 0, "bridge init should succeed");

    let listen_hash = CString::new("rln://listener-test").unwrap();
    let listen_task_id = reticulum_listen(listen_hash.as_ptr());
    assert!(listen_task_id > 0, "listen should return a positive task ID");

    let listener_handle = poll_task(listen_task_id, Duration::from_secs(5))
        .expect("listener task should complete quickly");
    assert!(listener_handle > 0);

    // Start accept — it blocks waiting for a connection.
    let accept_task_id = reticulum_accept(listener_handle);
    assert!(accept_task_id > 0);

    // Dial the same hash in-memory; this pushes a connection into the listener queue.
    let dial_hash = CString::new("rln://listener-test").unwrap();
    let dial_task_id = reticulum_dial(dial_hash.as_ptr());
    assert!(dial_task_id > 0);

    // Both tasks should complete now.
    let _dial_handle = poll_task(dial_task_id, Duration::from_secs(5))
        .expect("dial task should succeed (in-memory)");
    let _conn_handle = poll_task(accept_task_id, Duration::from_secs(5))
        .expect("accept task should succeed after dial");

    reticulum_close(listener_handle);
    reticulum_shutdown();
}

/// Test accepting with an invalid listener handle returns an error quickly.
#[test]
fn test_accept_invalid_handle() {
    let ret = reticulum_init(ptr::null());
    assert_eq!(ret, 0, "bridge init should succeed");

    // Handle 99999 doesn't exist — the spawned task should fail immediately.
    let task_id = reticulum_accept(99999);
    assert!(task_id >= 0, "accept should return a task ID");

    let result = poll_task(task_id, Duration::from_secs(2));
    assert!(result.is_err(), "accept on invalid handle should fail");

    reticulum_shutdown();
}
