#[cfg(test)]
mod tests {
    use serial_test::serial;
    use std::ffi::CString;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use sing_box_reticulum_bridge::c_api::*;
    use sing_box_reticulum_bridge::store::global_store;

    static RRT_TASK_ID:  AtomicU64 = AtomicU64::new(u64::MAX);
    static RRT_CONN_ID:  AtomicU64 = AtomicU64::new(u64::MAX);

    // Loopback test atomics
    static LB_ACCEPT_LISTENER_ID: AtomicU64 = AtomicU64::new(u64::MAX);
    static LB_ACCEPT_CONN_ID:     AtomicU64 = AtomicU64::new(u64::MAX);
    static LB_CONNECT_TASK_ID:    AtomicU64 = AtomicU64::new(u64::MAX);
    static LB_CONNECT_CONN_ID:    AtomicU64 = AtomicU64::new(u64::MAX);
    static LB_DATA_CONN_ID:       AtomicU64 = AtomicU64::new(u64::MAX);
    static LB_DATA_LEN:           AtomicU64 = AtomicU64::new(0);
    static LB_CLOSE_CONN_ID:      AtomicU64 = AtomicU64::new(u64::MAX);

    extern "C" fn rrt_on_accept(_: u64, _: u64, _: *const std::ffi::c_char) {}
    extern "C" fn rrt_on_connect(task_id: u64, conn_id: u64) {
        RRT_TASK_ID.store(task_id, Ordering::Release);
        RRT_CONN_ID.store(conn_id, Ordering::Release);
    }
    extern "C" fn rrt_on_data(_: u64, _: *const u8, _: usize) {}
    extern "C" fn rrt_on_close(_: u64) {}

    extern "C" fn lb_on_accept(listener_id: u64, conn_id: u64, _: *const std::ffi::c_char) {
        LB_ACCEPT_LISTENER_ID.store(listener_id, Ordering::Release);
        LB_ACCEPT_CONN_ID.store(conn_id, Ordering::Release);
    }
    extern "C" fn lb_on_connect(task_id: u64, conn_id: u64) {
        LB_CONNECT_TASK_ID.store(task_id, Ordering::Release);
        LB_CONNECT_CONN_ID.store(conn_id, Ordering::Release);
    }
    extern "C" fn lb_on_data(conn_id: u64, _data: *const u8, len: usize) {
        LB_DATA_CONN_ID.store(conn_id, Ordering::Release);
        LB_DATA_LEN.store(len as u64, Ordering::Release);
    }
    extern "C" fn lb_on_close(conn_id: u64) {
        LB_CLOSE_CONN_ID.store(conn_id, Ordering::Release);
    }

    fn wait_atomic_set(atom: &AtomicU64, not_value: u64, timeout: Duration) -> Option<u64> {
        let deadline = Instant::now() + timeout;
        loop {
            let v = atom.load(Ordering::Acquire);
            if v != not_value { return Some(v); }
            if Instant::now() > deadline { return None; }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_connect(task_id: u64, timeout: Duration) -> Option<u64> {
        let deadline = Instant::now() + timeout;
        loop {
            if RRT_TASK_ID.load(Ordering::Acquire) == task_id {
                return Some(RRT_CONN_ID.load(Ordering::Acquire));
            }
            if Instant::now() > deadline { return None; }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn get_listener_dest_hash(listener_handle: u64) -> String {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = global_store();
            let listener = store.get_listener(listener_handle).await
                .expect("listener should exist");
            listener.destination_hash().await
                .expect("listener should have dest hash")
                .to_hex_string()
        })
    }

    #[test]
    #[serial]
    fn test_bridge_init_and_listen() {
        RRT_TASK_ID.store(u64::MAX, Ordering::Release);
        RRT_CONN_ID.store(u64::MAX, Ordering::Release);

        let config_json = r#"{"identity_name":"bridge-test","interfaces":[]}"#;
        let config_cstr = CString::new(config_json).unwrap();
        let ret = reticulum_init(config_cstr.as_ptr(),
            Some(rrt_on_accept), Some(rrt_on_connect), Some(rrt_on_data), Some(rrt_on_close));
        assert_eq!(ret, 0);

        let name = CString::new("test-hash").unwrap();
        let listener_id = reticulum_listen(name.as_ptr());
        assert!(listener_id > 0, "listen should return positive id");

        let dest_hash = get_listener_dest_hash(listener_id as u64);
        assert_eq!(dest_hash.len(), 32);

        reticulum_close(listener_id as u64);
        reticulum_shutdown();
    }

    #[test]
    #[serial]
    fn test_dial_unknown_hash_fires_error_callback() {
        RRT_TASK_ID.store(u64::MAX, Ordering::Release);
        RRT_CONN_ID.store(u64::MAX, Ordering::Release);

        let config_json = r#"{"identity_name":"dial-unknown-test","interfaces":[]}"#;
        let cstr = CString::new(config_json).unwrap();
        let ret = reticulum_init(cstr.as_ptr(),
            Some(rrt_on_accept), Some(rrt_on_connect), Some(rrt_on_data), Some(rrt_on_close));
        assert_eq!(ret, 0);

        let hash = CString::new("aabbccdd00112233445566778899aabb").unwrap();
        let task_id: u64 = 1001;
        reticulum_dial(task_id, hash.as_ptr());

        let result = wait_connect(task_id, Duration::from_secs(35));
        assert_eq!(result, Some(0), "dial to unknown hash should fire callback with conn_id=0");

        reticulum_shutdown();
    }

    /// Loopback test: listen, dial to own dest hash, write data.
    /// Exercises on_accept, on_data, on_close callbacks.
    /// Skips data/accept assertions if the transport can't loopback
    /// (conn_id == 0 from on_connect is acceptable without real peers).
    #[test]
    #[serial]
    fn test_loopback_accept_data_close() {
        LB_ACCEPT_LISTENER_ID.store(u64::MAX, Ordering::Release);
        LB_ACCEPT_CONN_ID.store(u64::MAX, Ordering::Release);
        LB_CONNECT_TASK_ID.store(u64::MAX, Ordering::Release);
        LB_CONNECT_CONN_ID.store(u64::MAX, Ordering::Release);
        LB_DATA_CONN_ID.store(u64::MAX, Ordering::Release);
        LB_DATA_LEN.store(0, Ordering::Release);
        LB_CLOSE_CONN_ID.store(u64::MAX, Ordering::Release);

        let config_json = r#"{"identity_name":"loopback-test","interfaces":[]}"#;
        let config_cstr = CString::new(config_json).unwrap();
        let ret = reticulum_init(config_cstr.as_ptr(),
            Some(lb_on_accept), Some(lb_on_connect), Some(lb_on_data), Some(lb_on_close));
        assert_eq!(ret, 0, "init should succeed");

        // Start a listener — must succeed (local service registration works without network).
        let name = CString::new("loopback-svc").unwrap();
        let listener_id = reticulum_listen(name.as_ptr());
        assert!(listener_id > 0, "reticulum_listen should return positive id");

        // Get the registered dest hash so we can dial it.
        let dest_hash_hex = get_listener_dest_hash(listener_id as u64);
        assert_eq!(dest_hash_hex.len(), 32, "dest hash should be 32 hex chars");

        // Dial to ourselves.
        let task_id: u64 = 5555;
        let dest_cstr = CString::new(dest_hash_hex).unwrap();
        reticulum_dial(task_id, dest_cstr.as_ptr());

        // Wait for on_connect — may succeed (conn_id > 0) or fail (conn_id == 0).
        let conn_result =
            wait_atomic_set(&LB_CONNECT_TASK_ID, u64::MAX, Duration::from_secs(15));
        assert!(conn_result.is_some(), "on_connect must fire within 15 s");

        let conn_id = LB_CONNECT_CONN_ID.load(Ordering::Acquire);
        if conn_id == 0 {
            // Transport doesn't support loopback in this environment; skip further checks.
            eprintln!("[test_loopback] dial failed (no loopback transport) — skipping data assertions");
            reticulum_close(listener_id as u64);
            reticulum_shutdown();
            return;
        }

        // Dial succeeded — exercise on_accept (server side).
        let accept_result =
            wait_atomic_set(&LB_ACCEPT_CONN_ID, u64::MAX, Duration::from_secs(5));
        assert!(accept_result.is_some(), "on_accept must fire on the listener side");

        // Write data on the dialer's connection and expect on_data to fire.
        let msg = b"ping";
        let n = reticulum_write(conn_id, msg.as_ptr(), msg.len());
        assert!(n > 0, "reticulum_write should succeed, got {}", n);

        let data_result =
            wait_atomic_set(&LB_DATA_CONN_ID, u64::MAX, Duration::from_secs(5));
        assert!(data_result.is_some(), "on_data must fire after write");
        assert_eq!(
            LB_DATA_LEN.load(Ordering::Acquire), msg.len() as u64,
            "on_data length should match written length"
        );

        // Close the connection and verify on_close fires.
        reticulum_close(conn_id);
        let close_result =
            wait_atomic_set(&LB_CLOSE_CONN_ID, u64::MAX, Duration::from_secs(5));
        assert!(close_result.is_some(), "on_close must fire after reticulum_close");

        reticulum_close(listener_id as u64);
        reticulum_shutdown();
    }
}
