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
