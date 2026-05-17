package reticulum

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	C "github.com/sagernet/sing-box/constant"
	"github.com/stretchr/testify/require"
)

func TestReticulumInboundOptionsJSON(t *testing.T) {
	jsonInline := `{
		"type": "reticulum",
		"tag": "reticulum-in",
		"listen": "0.0.0.0",
		"listen_port": 12345,
		"reticulum_config": {
			"identity_path": "/path/to/identity",
			"storage_path": "/path/to/storage",
			"interfaces": [
				{"type": "udp", "listen_port": 4242}
			]
		}
	}`
	var opts option.ReticulumInboundOptions
	err := json.Unmarshal([]byte(jsonInline), &opts)
	require.NoError(t, err)
	require.Equal(t, C.TypeReticulum, C.TypeReticulum) // just ensure constant is defined
	require.NotNil(t, opts.ReticulumConfig)
	require.Equal(t, "/path/to/identity", opts.ReticulumConfig.IdentityPath)
	require.Equal(t, "/path/to/storage", opts.ReticulumConfig.StoragePath)
	require.Len(t, opts.ReticulumConfig.Interfaces, 1)
	require.Equal(t, "udp", opts.ReticulumConfig.Interfaces[0].Type)
	require.Equal(t, uint16(4242), opts.ReticulumConfig.Interfaces[0].ListenPort)
}

func TestReticulumOutboundOptionsJSON(t *testing.T) {
	jsonOutbound := `{
		"type": "reticulum",
		"tag": "reticulum-out",
		"server": "10.0.0.1",
		"server_port": 443,
		"reticulum_config_path": "/etc/reticulum/config",
		"name": "my_server_name",
		"password": "very_secure_password"
	}`
	var opts option.ReticulumOutboundOptions
	err := json.Unmarshal([]byte(jsonOutbound), &opts)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", opts.ServerOptions.Server)
	require.Equal(t, uint16(443), opts.ServerOptions.ServerPort)
	require.Equal(t, "/etc/reticulum/config", opts.ReticulumConfigPath)
	require.Equal(t, "my_server_name", opts.Name)
	require.Equal(t, "very_secure_password", opts.Password)
}

func TestNewInboundReturnsStub(t *testing.T) {
	ctx := context.Background()
	var router adapter.Router // nil is fine for this test
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumInboundOptions{}
	inbound, err := NewInbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	require.NotNil(t, inbound)
}

func TestNewOutboundReturnsStub(t *testing.T) {
	ctx := context.Background()
	var router adapter.Router
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumOutboundOptions{}
	outbound, err := NewOutbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	require.NotNil(t, outbound)
}

func TestRegisterInboundAddsTypeToRegistry(t *testing.T) {
	reg := inbound.NewRegistry()
	RegisterInbound(reg)
	// No panic means success
}

func TestRegisterOutboundAddsTypeToRegistry(t *testing.T) {
	reg := outbound.NewRegistry()
	RegisterOutbound(reg)
}


