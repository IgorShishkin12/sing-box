/// Test that parse_config handles valid JSON and returns Err on invalid JSON.
#[test]
fn test_config_valid_json() {
    let valid_json = r#"{
        "identity_path": "/tmp/test",
        "storage_path": "/tmp/test",
        "interfaces": [
            {"type": "udp", "port": 4242}
        ]
    }"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(valid_json);
    assert!(cfg.is_ok(), "valid JSON should parse successfully");

    let config = cfg.unwrap();
    assert_eq!(config.identity_path, Some("/tmp/test".to_string()));
    assert_eq!(config.storage_path, Some("/tmp/test".to_string()));
    assert_eq!(config.interfaces.len(), 1);
    assert_eq!(config.interfaces[0].r#type, "udp");
    assert_eq!(config.interfaces[0].port, Some(4242));
}

#[test]
fn test_config_valid_json_minimal() {
    // Minimal valid config without optional fields
    let valid_json = r#"{}"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(valid_json);
    assert!(cfg.is_ok(), "empty object should parse successfully");
    let config = cfg.unwrap();
    assert!(config.identity_path.is_none());
    assert!(config.storage_path.is_none());
    assert!(config.interfaces.is_empty(), "interfaces should default to empty vec");
}

#[test]
fn test_config_invalid_json() {
    let invalid_json = r#"not valid json"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(invalid_json);
    assert!(cfg.is_err(), "invalid JSON should return Err");
}

#[test]
fn test_config_empty_string_fails() {
    let cfg = sing_box_reticulum_bridge::config::parse_config("");
    assert!(cfg.is_err(), "empty string should return Err");
}