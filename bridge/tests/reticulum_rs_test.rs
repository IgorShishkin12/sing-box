#[cfg(feature = "real-reticulum")]
#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;
    use std::time::Duration;

    use sing_box_reticulum_bridge::c_api::*;
    use sing_box_reticulum_bridge::store::global_store;

    /// Helper: poll a task until it completes (or timeout).
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
                    let handle_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                    if handle_bytes.len() != 8 {
                        reticulum_free(result_out);
                        return Err(format!("unexpected handle byte length: {}", handle_bytes.len()));
                    }
                    let handle = u64::from_le_bytes(handle_bytes.try_into().unwrap());
                    reticulum_free(result_out);
                    return Ok(handle);
                }
                -1 => {
                    if !result_out.is_null() {
                        let msg_bytes = unsafe { std::slice::from_raw_parts(result_out, len_out) };
                        let msg = String::from_utf8_lossy(msg_bytes).to_string();
                        reticulum_free(result_out);
                        return Err(msg);
                    }
                    return Err("unknown error".to_string());
                }
                _ => {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    /// Helper: get the destination address hash from a listener handle.
    fn get_listener_dest_hash(listener_handle: u64) -> String {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = global_store();
            let listener = store
                .get_listener(listener_handle)
                .await
                .expect("listener should exist in store");
            let dest_hash = listener
                .destination_hash()
                .await
                .expect("listener should have a destination hash when real-reticulum is active");
            dest_hash.to_hex_string()
        })
    }

    /// Test: bridge init and listen with real-reticulum (no interfaces).
    #[test]
    fn test_bridge_init_and_listen() {
        let config_json = r#"{
            "identity_name": "bridge-test",
            "interfaces": []
        }"#;
        let config_cstr = CString::new(config_json).unwrap();

        let ret = reticulum_init(config_cstr.as_ptr());
        assert_eq!(ret, 0, "bridge init should succeed");

        let listen_hash = CString::new("test-hash").unwrap();
        let listen_task_id = reticulum_listen(listen_hash.as_ptr());
        assert!(listen_task_id >= 0);

        let listener_handle = poll_task(listen_task_id, Duration::from_secs(10))
            .expect("listener task should complete");
        eprintln!("[test] listener_handle={}", listener_handle);

        let dest_hash_hex = get_listener_dest_hash(listener_handle);
        eprintln!("[test] dest_hash_hex={}", dest_hash_hex);
        assert_eq!(dest_hash_hex.len(), 32);

        reticulum_close(listener_handle);
        reticulum_shutdown();

        eprintln!("[test] bridge init and listen test PASSED");
    }

    /// Test: dial to an unknown hash returns an error.
    #[test]
    fn test_dial_unknown_hash() {
        let config_json = r#"{
            "identity_name": "dial-unknown-test",
            "interfaces": []
        }"#;
        let config_cstr = CString::new(config_json).unwrap();

        let ret = reticulum_init(config_cstr.as_ptr());
        assert_eq!(ret, 0);

        let unknown_hash = CString::new("aabbccdd00112233445566778899aabb").unwrap();
        let dial_task_id = reticulum_dial(unknown_hash.as_ptr());
        assert!(dial_task_id >= 0);

        let result = poll_task(dial_task_id, Duration::from_secs(10));
        assert!(result.is_err(), "dial to unknown hash should fail, got {:?}", result);

        reticulum_shutdown();
    }

    /// Test: bridge works with real-reticulum (no interfaces).
    #[test]
    fn test_bridge_listen_and_dial() {
        let config_json = r#"{
            "identity_name": "listen-dial-test",
            "interfaces": []
        }"#;
        let config_cstr = CString::new(config_json).unwrap();

        let ret = reticulum_init(config_cstr.as_ptr());
        assert_eq!(ret, 0);

        let listen_hash = CString::new("listen-dial-hash").unwrap();
        let listen_task_id = reticulum_listen(listen_hash.as_ptr());
        assert!(listen_task_id >= 0);
        let listener_handle = poll_task(listen_task_id, Duration::from_secs(10))
            .expect("listener task should complete");

        let dest_hash_hex = get_listener_dest_hash(listener_handle);

        // Without interfaces, dial should fail (no transport peer to discover).
        // This tests the error path without hanging.
        let dial_hash_cstr = CString::new(dest_hash_hex).unwrap();
        let dial_task_id = reticulum_dial(dial_hash_cstr.as_ptr());

        if dial_task_id >= 0 {
            let dial_result = poll_task(dial_task_id, Duration::from_secs(15));
            eprintln!("[test] dial result (expected to fail without network): {:?}", dial_result);
        }

        reticulum_close(listener_handle);
        reticulum_shutdown();
    }
}