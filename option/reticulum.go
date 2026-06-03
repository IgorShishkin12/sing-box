package option

type ReticulumInboundOptions struct {
	ListenOptions
	Network             NetworkList      `json:"network,omitempty"`
	ReticulumConfig     *ReticulumConfig `json:"reticulum_config,omitempty"`
	ReticulumConfigPath string           `json:"reticulum_config_path,omitempty"`
	Destination         string           `json:"destination,omitempty"`
	Name                string           `json:"name,omitempty"`
	Password            string           `json:"password,omitempty"`
	AuthRetry           string           `json:"auth_retry,omitempty"`
}

type ReticulumConfig struct {
	IdentityPath        string               `json:"identity_path,omitempty"`
	StoragePath         string               `json:"storage_path,omitempty"`
	ConfigDir           string               `json:"config_dir,omitempty"`
	IdentityKey         string               `json:"identity_key,omitempty"`
	IdentityName        string               `json:"identity_name,omitempty"`
	ReticulumConfigPath string               `json:"reticulum_config_path,omitempty"`
	Interfaces          []ReticulumInterface `json:"interfaces,omitempty"`
}

type ReticulumInterface struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type"`
	// UDPInterface
	ListenIP    string `json:"listen_ip,omitempty"`
	ListenPort  uint16 `json:"listen_port,omitempty"`
	ForwardIP   string `json:"forward_ip,omitempty"`
	ForwardPort uint16 `json:"forward_port,omitempty"`
	// TCPClientInterface
	TargetHost string `json:"target_host,omitempty"`
	TargetPort uint16 `json:"target_port,omitempty"`
	// AutoInterface
	DataPort uint16 `json:"data_port,omitempty"`
}

type ReticulumOutboundOptions struct {
	DialerOptions
	ServerOptions
	Network             NetworkList      `json:"network,omitempty"`
	ReticulumConfig     *ReticulumConfig `json:"reticulum_config,omitempty"`
	ReticulumConfigPath string           `json:"reticulum_config_path,omitempty"`
	Destination         string           `json:"destination,omitempty"`
	Name                string           `json:"name,omitempty"`
	Password            string           `json:"password,omitempty"`
	AuthRetry           string           `json:"auth_retry,omitempty"`
}
