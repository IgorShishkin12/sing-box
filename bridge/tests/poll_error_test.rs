/// Test that reticulum_init with null callbacks succeeds (callbacks are optional).
#[test]
fn test_init_with_null_callbacks_succeeds() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(
        config.as_ptr(),
        None, // on_accept
        None, // on_connect
        None, // on_data
        None, // on_close
    );
    assert_eq!(ret, 0, "bridge init with null callbacks should succeed");
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// reticulum_close on an unknown handle is a no-op.
#[test]
fn test_close_unknown_handle_is_noop() {
    sing_box_reticulum_bridge::c_api::reticulum_close(99999);
    // No panic or undefined behaviour.
}
