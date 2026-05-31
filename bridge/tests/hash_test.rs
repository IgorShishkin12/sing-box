use serial_test::serial;
use std::ffi::CString;

/// Helper: init with fresh runtime for each test
fn bridge_init() {
    let config = std::ffi::CString::new("{}").unwrap();
    let ret = unsafe {
        sing_box_reticulum_bridge::c_api::reticulum_init(config.as_ptr(), None, None, None, None)
    };
    assert_eq!(ret, 0);
}

/// Helper: clean shutdown
fn bridge_shutdown() {
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that get_hash returns -1 for an unknown name
#[test]
#[serial]
fn test_get_hash_unknown_name() {
    bridge_init();

    let name = CString::new("nonexistent").unwrap();
    let mut hash_out: *mut std::ffi::c_char = std::ptr::null_mut();
    let result =
        unsafe { sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out, name.as_ptr()) };
    assert_eq!(result, -1, "unknown name should return -1");
    assert!(
        hash_out.is_null(),
        "hash pointer should be null for unknown name"
    );

    bridge_shutdown();
}

/// Test that get_hash returns the registered hash after the name is registered.
#[test]
#[serial]
fn test_get_hash_after_register() {
    bridge_init();

    let name = CString::new("alice").unwrap();
    let expected_hash = CString::new("abc123def456").unwrap();
    let reg_ret = unsafe {
        sing_box_reticulum_bridge::c_api::reticulum_register_name(
            name.as_ptr(),
            expected_hash.as_ptr(),
        )
    };
    assert_eq!(reg_ret, 0, "register should succeed");

    let mut hash_out: *mut std::ffi::c_char = std::ptr::null_mut();
    let result =
        unsafe { sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out, name.as_ptr()) };
    assert_eq!(result, 0, "get_hash should succeed for registered name");
    assert!(!hash_out.is_null(), "hash pointer should not be null");

    let hash_str = unsafe {
        std::ffi::CStr::from_ptr(hash_out)
            .to_string_lossy()
            .into_owned()
    };
    assert_eq!(
        hash_str, "abc123def456",
        "returned hash must match the registered hash"
    );

    sing_box_reticulum_bridge::c_api::reticulum_free(hash_out as *mut u8);
    bridge_shutdown();
}

/// Test get_hash with null parameters
#[test]
fn test_get_hash_null_params() {
    // No need to init for null checks
    let result = unsafe {
        sing_box_reticulum_bridge::c_api::get_hash(std::ptr::null_mut(), std::ptr::null())
    };
    assert_eq!(result, -1, "null params should return -1");
}

/// Test that register_name with null parameters returns -1
#[test]
fn test_register_name_null_params() {
    let result = unsafe {
        sing_box_reticulum_bridge::c_api::reticulum_register_name(
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert_eq!(result, -1, "null params should return -1");
}

/// Test that get_hash returns the same value on repeated lookups.
#[test]
#[serial]
fn test_get_hash_deterministic() {
    bridge_init();

    let name = CString::new("bob").unwrap();
    let registered = CString::new("somehash").unwrap();
    let reg_ret = unsafe {
        sing_box_reticulum_bridge::c_api::reticulum_register_name(
            name.as_ptr(),
            registered.as_ptr(),
        )
    };
    assert_eq!(reg_ret, 0);

    let mut hash_out1: *mut std::ffi::c_char = std::ptr::null_mut();
    let _ = unsafe { sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out1, name.as_ptr()) };
    let hash1 = unsafe {
        std::ffi::CStr::from_ptr(hash_out1)
            .to_string_lossy()
            .into_owned()
    };
    sing_box_reticulum_bridge::c_api::reticulum_free(hash_out1 as *mut u8);

    let mut hash_out2: *mut std::ffi::c_char = std::ptr::null_mut();
    let _ = unsafe { sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out2, name.as_ptr()) };
    let hash2 = unsafe {
        std::ffi::CStr::from_ptr(hash_out2)
            .to_string_lossy()
            .into_owned()
    };
    sing_box_reticulum_bridge::c_api::reticulum_free(hash_out2 as *mut u8);

    assert_eq!(hash1, hash2, "repeated lookup should return same hash");
    assert_eq!(
        hash1, "somehash",
        "returned hash must match the registered value"
    );

    bridge_shutdown();
}
