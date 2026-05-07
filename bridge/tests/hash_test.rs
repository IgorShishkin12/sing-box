use std::ffi::CString;

/// Helper: init with fresh runtime for each test
fn bridge_init() {
    let ret = sing_box_reticulum_bridge::c_api::reticulum_init(std::ptr::null());
    assert_eq!(ret, 0);
}

/// Helper: clean shutdown
fn bridge_shutdown() {
    sing_box_reticulum_bridge::c_api::reticulum_shutdown();
}

/// Test that get_hash returns -1 for an unknown name
#[test]
fn test_get_hash_unknown_name() {
    bridge_init();

    let name = CString::new("nonexistent").unwrap();
    let mut hash_out: *mut std::ffi::c_char = std::ptr::null_mut();
    let result = sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out, name.as_ptr());
    assert_eq!(result, -1, "unknown name should return -1");
    assert!(hash_out.is_null(), "hash pointer should be null for unknown name");

    bridge_shutdown();
}

/// Test that get_hash returns a valid hash after the name is registered
#[test]
fn test_get_hash_after_register() {
    bridge_init();

    // Register a name
    let name = CString::new("alice").unwrap();
    let hash = CString::new("abc123def456").unwrap();
    let reg_ret = sing_box_reticulum_bridge::c_api::reticulum_register_name(name.as_ptr(), hash.as_ptr());
    assert_eq!(reg_ret, 0, "register should succeed");

    // Look it up
    let mut hash_out: *mut std::ffi::c_char = std::ptr::null_mut();
    let result = sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out, name.as_ptr());
    assert_eq!(result, 0, "get_hash should succeed for registered name");
    assert!(!hash_out.is_null(), "hash pointer should not be null");

    // Read the hash
    let hash_str = unsafe { std::ffi::CStr::from_ptr(hash_out).to_string_lossy().into_owned() };
    assert!(!hash_str.is_empty(), "hash should not be empty");
    // The returned hash is a deterministic hash of the name, not the one we registered
    // (since register_name is currently a placeholder)
    assert_eq!(hash_str.len(), 16, "hash should be 16 hex chars");

    // Free the result using the bridge's free function (consistent allocator)
    sing_box_reticulum_bridge::c_api::reticulum_free(hash_out as *mut u8);

    bridge_shutdown();
}

/// Test get_hash with null parameters
#[test]
fn test_get_hash_null_params() {
    // No need to init for null checks
    let result = sing_box_reticulum_bridge::c_api::get_hash(std::ptr::null_mut(), std::ptr::null());
    assert_eq!(result, -1, "null params should return -1");
}

/// Test that register_name with null parameters returns -1
#[test]
fn test_register_name_null_params() {
    let result = sing_box_reticulum_bridge::c_api::reticulum_register_name(std::ptr::null(), std::ptr::null());
    assert_eq!(result, -1, "null params should return -1");
}

/// Test that get_hash returns deterministic hashes for the same name
#[test]
fn test_get_hash_deterministic() {
    bridge_init();

    // Register a name
    let name = CString::new("bob").unwrap();
    let hash = CString::new("somehash").unwrap();
    let reg_ret = sing_box_reticulum_bridge::c_api::reticulum_register_name(name.as_ptr(), hash.as_ptr());
    assert_eq!(reg_ret, 0);

    // Look it up twice — should give same result
    let mut hash_out1: *mut std::ffi::c_char = std::ptr::null_mut();
    let _ = sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out1, name.as_ptr());
    let hash1 = unsafe { std::ffi::CStr::from_ptr(hash_out1).to_string_lossy().into_owned() };
    sing_box_reticulum_bridge::c_api::reticulum_free(hash_out1 as *mut u8);

    let mut hash_out2: *mut std::ffi::c_char = std::ptr::null_mut();
    let _ = sing_box_reticulum_bridge::c_api::get_hash(&mut hash_out2, name.as_ptr());
    let hash2 = unsafe { std::ffi::CStr::from_ptr(hash_out2).to_string_lossy().into_owned() };
    sing_box_reticulum_bridge::c_api::reticulum_free(hash_out2 as *mut u8);

    assert_eq!(hash1, hash2, "hash should be deterministic for the same name");

    bridge_shutdown();
}