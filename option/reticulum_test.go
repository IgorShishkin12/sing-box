package option_test

import (
	"testing"

	"github.com/sagernet/sing-box/option"
)

func TestValidate_AutoInterface(t *testing.T) {
	cfg := &option.ReticulumConfig{
		Interfaces: []option.ReticulumInterface{
			{Type: "AutoInterface"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("AutoInterface should be valid, got: %v", err)
	}
}

func TestValidate_AutoInterface_WithDataPort(t *testing.T) {
	cfg := &option.ReticulumConfig{
		Interfaces: []option.ReticulumInterface{
			{Type: "AutoInterface", DataPort: 49555},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("AutoInterface with data_port should be valid, got: %v", err)
	}
}

func TestValidate_EmptyType_Fails(t *testing.T) {
	cfg := &option.ReticulumConfig{
		Interfaces: []option.ReticulumInterface{
			{Type: ""},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty type should fail validation")
	}
}

func TestValidate_NoInterfaces(t *testing.T) {
	cfg := &option.ReticulumConfig{}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty interfaces should be valid: %v", err)
	}
}

func TestValidate_RNodeSerial_RequiresDevice(t *testing.T) {
	cfg := &option.ReticulumConfig{
		Interfaces: []option.ReticulumInterface{{Type: "RNodeSerial"}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("RNodeSerial without device should fail")
	}
}

func TestValidate_RNodeSerial_Valid(t *testing.T) {
	cfg := &option.ReticulumConfig{
		Interfaces: []option.ReticulumInterface{
			{Type: "RNodeSerial", Device: "/dev/ttyUSB0"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid RNodeSerial: %v", err)
	}
}

func TestValidate_RNodeSerial_LoraFields(t *testing.T) {
	base := option.ReticulumInterface{Type: "RNodeSerial", Device: "/dev/ttyUSB0"}

	cases := []struct {
		name  string
		mutFn func(*option.ReticulumInterface)
		wantErr bool
	}{
		{"sf too high", func(i *option.ReticulumInterface) { i.SpreadingFactor = 13 }, true},
		{"sf too low", func(i *option.ReticulumInterface) { i.SpreadingFactor = 4 }, true},
		{"sf valid", func(i *option.ReticulumInterface) { i.SpreadingFactor = 7 }, false},
		{"cr too low", func(i *option.ReticulumInterface) { i.CodingRate = 4 }, true},
		{"cr too high", func(i *option.ReticulumInterface) { i.CodingRate = 9 }, true},
		{"cr valid", func(i *option.ReticulumInterface) { i.CodingRate = 6 }, false},
		{"txpower too high", func(i *option.ReticulumInterface) { i.TxPowerDBm = 38 }, true},
		{"txpower valid", func(i *option.ReticulumInterface) { i.TxPowerDBm = 17 }, false},
		{"freq too low", func(i *option.ReticulumInterface) { i.FrequencyHz = 100_000_000 }, true},
		{"freq too high", func(i *option.ReticulumInterface) { i.FrequencyHz = 4_000_000_000 }, true},
		{"freq valid EU868", func(i *option.ReticulumInterface) { i.FrequencyHz = 868_000_000 }, false},
		{"bw too low", func(i *option.ReticulumInterface) { i.BandwidthHz = 7_000 }, true},
		{"bw too high", func(i *option.ReticulumInterface) { i.BandwidthHz = 2_000_000 }, true},
		{"bw valid", func(i *option.ReticulumInterface) { i.BandwidthHz = 125_000 }, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := base
			tc.mutFn(&i)
			err := (&option.ReticulumConfig{Interfaces: []option.ReticulumInterface{i}}).Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %q", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.name, err)
			}
		})
	}
}
