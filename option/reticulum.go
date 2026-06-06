package option

import "fmt"

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

// Validate checks each interface entry for configuration errors.
// Interface-type-specific required fields are enforced here so errors are
// reported before the config is marshaled and sent to the Rust bridge.
func (c *ReticulumConfig) Validate() error {
	for i, iface := range c.Interfaces {
		if iface.Type == "" {
			return fmt.Errorf("interfaces[%d].type is required", i)
		}
	}
	return nil
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
	AuthOnStart         bool             `json:"auth_on_start,omitempty"`
}
