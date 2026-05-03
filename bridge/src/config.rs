use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReticulumConfig {
    pub identity_path: Option<String>,
    pub storage_path: Option<String>,
    pub interfaces: Vec<ReticulumInterface>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReticulumInterface {
    pub r#type: String,
    pub port: Option<u16>,
}

pub fn parse_config(json_str: &str) -> Result<ReticulumConfig, serde_json::Error> {
    serde_json::from_str(json_str)
}
