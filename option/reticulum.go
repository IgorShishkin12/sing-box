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
	// RustLog sets the RUST_LOG tracing filter for the Reticulum bridge.
	// If empty, the level is derived from the sing-box log level. Ignored
	// when RUST_LOG is already set in the environment.
	// Example: "trace,btleplug=debug,serde=off,jni=off"
	RustLog string `json:"rust_log,omitempty"`
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
	// RNodeSerial: serial device path (e.g. /dev/ttyUSB0)
	Device string `json:"device,omitempty"`
	// RNodeBLE: BLE peripheral identifier (name or MAC address)
	PeripheralID string `json:"peripheral_id,omitempty"`
	// Shared LoRa radio parameters (RNodeSerial + RNodeBLE).
	// Unset fields default to US915 band values.
	FrequencyHz     uint64 `json:"frequency_hz,omitempty"`
	BandwidthHz     uint32 `json:"bandwidth_hz,omitempty"`
	TxPowerDBm      int8   `json:"tx_power_dbm,omitempty"`
	SpreadingFactor uint8  `json:"spreading_factor,omitempty"`
	// CodingRate as an integer 5–8 (maps to 4/5 … 4/8).
	CodingRate uint8 `json:"coding_rate,omitempty"`
}

// Validate checks each interface entry for configuration errors.
// Interface-type-specific required fields are enforced here so errors are
// reported before the config is marshaled and sent to the Rust bridge.
func (c *ReticulumConfig) Validate() error {
	for i, iface := range c.Interfaces {
		if iface.Type == "" {
			return fmt.Errorf("interfaces[%d].type is required", i)
		}
		if err := iface.validate(i); err != nil {
			return err
		}
	}
	return nil
}

func (iface ReticulumInterface) validate(index int) error {
	switch iface.Type {
	case "RNodeSerial":
		if iface.Device == "" {
			return fmt.Errorf("interfaces[%d].device is required for RNodeSerial", index)
		}
	case "RNodeBLE":
		if iface.PeripheralID == "" {
			return fmt.Errorf("interfaces[%d].peripheral_id is required for RNodeBLE", index)
		}
	}
	return iface.validateLoraFields(index)
}

const (
	loraFreqMin uint64 = 137_000_000
	loraFreqMax uint64 = 3_000_000_000
	loraBwMin   uint32 = 7_800
	loraBwMax   uint32 = 1_625_000
)

func (iface ReticulumInterface) validateLoraFields(index int) error {
	if iface.FrequencyHz != 0 && (iface.FrequencyHz < loraFreqMin || iface.FrequencyHz > loraFreqMax) {
		return fmt.Errorf("interfaces[%d].frequency_hz must be between %d and %d", index, loraFreqMin, loraFreqMax)
	}
	if iface.BandwidthHz != 0 && (iface.BandwidthHz < loraBwMin || iface.BandwidthHz > loraBwMax) {
		return fmt.Errorf("interfaces[%d].bandwidth_hz must be between %d and %d", index, loraBwMin, loraBwMax)
	}
	if iface.SpreadingFactor != 0 && (iface.SpreadingFactor < 5 || iface.SpreadingFactor > 12) {
		return fmt.Errorf("interfaces[%d].spreading_factor must be between 5 and 12", index)
	}
	if iface.CodingRate != 0 && (iface.CodingRate < 5 || iface.CodingRate > 8) {
		return fmt.Errorf("interfaces[%d].coding_rate must be between 5 and 8", index)
	}
	if iface.TxPowerDBm != 0 && (iface.TxPowerDBm < 0 || iface.TxPowerDBm > 37) {
		return fmt.Errorf("interfaces[%d].tx_power_dbm must be between 0 and 37", index)
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
