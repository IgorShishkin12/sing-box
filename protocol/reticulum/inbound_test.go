//go:build with_reticulum

package reticulum

import (
	"context"
	"os"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestInboundStartCreatesListener(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	ctx := context.Background()
	var router adapter.Router
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumInboundOptions{
		Name: "test-inbound",
	}

	inbound, err := NewInbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	require.NotNil(t, inbound)

	i := inbound.(*Inbound)

	err = i.Start(adapter.StartStateInitialize)
	require.NoError(t, err)
	require.NotZero(t, i.listenerHdl)
	require.True(t, i.accepting)

	i.Close()
	BridgeShutdown()
}

func TestInboundCloseStopsAcceptLoop(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	ctx := context.Background()
	var router adapter.Router
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumInboundOptions{
		Name: "test-inbound-close",
	}

	inbound, err := NewInbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	i := inbound.(*Inbound)

	err = i.Start(adapter.StartStateInitialize)
	require.NoError(t, err)
	require.True(t, i.accepting)

	// Close should stop the accept loop
	err = i.Close()
	require.NoError(t, err)
	require.False(t, i.accepting)
	require.True(t, i.closed)

	BridgeShutdown()
}

func TestInboundStartWithConfigPath(t *testing.T) {
	ctx := context.Background()
	var router adapter.Router
	logger := log.NewNOPFactory().Logger()

	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"
	configContent := `{"identity_path": "/tmp/test", "storage_path": "/tmp/test"}`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	opts := option.ReticulumInboundOptions{
		ReticulumConfigPath: configPath,
		Name:                "test-inbound-config",
	}

	inbound, err := NewInbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	i := inbound.(*Inbound)

	err = i.Start(adapter.StartStateInitialize)
	require.NoError(t, err)

	i.Close()
	BridgeShutdown()
}
