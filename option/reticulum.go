package option

type ReticulumInboundOptions struct {
	ListenOptions
	Network      NetworkList `json:"network,omitempty"`
	ReticulumConfig *ReticulumConfig `json:"reticulum_config,omitempty"`
	ReticulumConfigPath string `json:"reticulum_config_path,omitempty"`
	Destination  string `json:"destination,omitempty"`
	Name         string `json:"name,omitempty"`
	Password     string `json:"password,omitempty"`
}

type ReticulumConfig struct {
	IdentityPath string `json:"identity_path,omitempty"`
	StoragePath  string `json:"storage_path,omitempty"`
	Interfaces   []ReticulumInterface `json:"interfaces,omitempty"`
}

type ReticulumInterface struct {
	Type string `json:"type"`
	Port int    `json:"port,omitempty"`
}

type ReticulumOutboundOptions struct {
	DialerOptions
	ServerOptions
	Network       NetworkList `json:"network,omitempty"`
	ReticulumConfig *ReticulumConfig `json:"reticulum_config,omitempty"`
	ReticulumConfigPath string `json:"reticulum_config_path,omitempty"`
	Name         string `json:"name,omitempty"`
	Password     string `json:"password,omitempty"`
}
