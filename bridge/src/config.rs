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
}

pub fn parse_config(json_str: &str) -> Result<ReticulumConfig, serde_json::Error> {
    serde_json::from_str(json_str)
}
