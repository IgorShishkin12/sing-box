use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReticulumConfig {
    pub identity_path: Option<String>,
    pub storage_path: Option<String>,
    #[serde(default)]
    pub interfaces: Vec<ReticulumInterface>,
    /// Reticulum configuration directory (optional, defaults to ~/.reticulum).
    #[serde(default)]
    pub config_dir: Option<String>,
    /// 128-character hex private key (optional).
    #[serde(default)]
    pub identity_key: Option<String>,
    /// Human-readable name for generating a random identity (optional).
    #[serde(default)]
    pub identity_name: Option<String>,
    /// Reticulum-style INI config path inside config_dir (optional, defaults to
    /// `<config_dir>/config`).
    #[serde(default)]
    pub reticulum_config_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReticulumInterface {
    /// Free-form label (e.g. "My UDP Interface").
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub iface_type: String,
    // UDPInterface
    pub listen_ip: Option<String>,
    pub listen_port: Option<u16>,
    pub forward_ip: Option<String>,
    pub forward_port: Option<u16>,
    // TCPClientInterface
    pub target_host: Option<String>,
    pub target_port: Option<u16>,
    // AutoInterface
    pub data_port: Option<u16>,
    // RNodeSerial: serial device path (e.g. "/dev/ttyUSB0")
    pub device: Option<String>,
    // Shared LoRa radio parameters (RNodeSerial + RNodeBLE).
    // Unset fields default to US915 band values via LoraConfig::us915_default().
    pub frequency_hz: Option<u64>,
    pub bandwidth_hz: Option<u32>,
    pub tx_power_dbm: Option<i8>,
    pub spreading_factor: Option<u8>,
    /// Coding rate as an integer 5–8 (mapping to 4/5 … 4/8).
    pub coding_rate: Option<u8>,
}

pub fn parse_config(json_str: &str) -> Result<ReticulumConfig, serde_json::Error> {
    serde_json::from_str(json_str)
}
