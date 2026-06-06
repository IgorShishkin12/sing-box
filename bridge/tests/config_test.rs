/// Test that parse_config handles valid JSON and returns Err on invalid JSON.
#[test]
fn test_config_valid_json() {
    let valid_json = r#"{
        "identity_path": "/tmp/test",
        "storage_path": "/tmp/test",
        "interfaces": [
            {"name": "Test UDP", "type": "UDPInterface",
             "listen_ip": "0.0.0.0", "listen_port": 4242,
             "forward_ip": "peer", "forward_port": 4242}
        ]
    }"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(valid_json);
    assert!(cfg.is_ok(), "valid JSON should parse successfully");

    let config = cfg.unwrap();
    assert_eq!(config.identity_path, Some("/tmp/test".to_string()));
    assert_eq!(config.storage_path, Some("/tmp/test".to_string()));
    assert_eq!(config.interfaces.len(), 1);
    assert_eq!(config.interfaces[0].iface_type, "UDPInterface");
    assert_eq!(config.interfaces[0].listen_port, Some(4242));
    assert_eq!(config.interfaces[0].forward_ip, Some("peer".to_string()));
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
    assert!(
        config.interfaces.is_empty(),
        "interfaces should default to empty vec"
    );
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

#[test]
fn test_config_auto_interface_parses() {
    let json = r#"{"interfaces": [{"type": "AutoInterface", "data_port": 49555}]}"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(json).unwrap();
    assert_eq!(cfg.interfaces[0].iface_type, "AutoInterface");
    assert_eq!(cfg.interfaces[0].data_port, Some(49555));
}

#[test]
fn test_config_auto_interface_default_port() {
    let json = r#"{"interfaces": [{"type": "AutoInterface"}]}"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(json).unwrap();
    assert!(cfg.interfaces[0].data_port.is_none());
}

#[test]
fn test_config_rnode_serial_parses() {
    let json = r#"{
        "interfaces": [{
            "type": "RNodeSerial",
            "device": "/dev/ttyUSB0",
            "frequency_hz": 868000000,
            "bandwidth_hz": 125000,
            "tx_power_dbm": 14,
            "spreading_factor": 9,
            "coding_rate": 5
        }]
    }"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(json).unwrap();
    let iface = &cfg.interfaces[0];
    assert_eq!(iface.iface_type, "RNodeSerial");
    assert_eq!(iface.device, Some("/dev/ttyUSB0".to_string()));
    assert_eq!(iface.frequency_hz, Some(868_000_000));
    assert_eq!(iface.bandwidth_hz, Some(125_000));
    assert_eq!(iface.tx_power_dbm, Some(14));
    assert_eq!(iface.spreading_factor, Some(9));
    assert_eq!(iface.coding_rate, Some(5));
}

#[test]
fn test_config_rnode_serial_defaults_optional() {
    let json = r#"{"interfaces": [{"type": "RNodeSerial", "device": "/dev/ttyUSB0"}]}"#;
    let cfg = sing_box_reticulum_bridge::config::parse_config(json).unwrap();
    let iface = &cfg.interfaces[0];
    assert!(iface.frequency_hz.is_none());
    assert!(iface.spreading_factor.is_none());
    assert!(iface.coding_rate.is_none());
}
