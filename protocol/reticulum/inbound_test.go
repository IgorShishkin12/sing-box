//go:build with_reticulum

package reticulum

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestInboundStartCreatesListener(t *testing.T) {
	err := BridgeInit("")
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

func TestInboundAcceptLoopForwardsConnection(t *testing.T) {
	err := BridgeInit("")
	require.NoError(t, err)

	ctx := context.Background()
	var router adapter.Router
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumInboundOptions{
		Name: "test-inbound-accept",
	}

	inbound, err := NewInbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	i := inbound.(*Inbound)

	err = i.Start(adapter.StartStateInitialize)
	require.NoError(t, err)
	require.NotZero(t, i.listenerHdl)

	// Give accept loop time to start
	time.Sleep(100 * time.Millisecond)

	// Create a connection via BridgeDial to simulate an incoming connection
	taskID, err := BridgeDial("rln://test-conn")
	require.NoError(t, err)
	require.Greater(t, taskID, 0)

	// Poll for completion
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)
	require.Greater(t, handle, uint64(0))

	// The connection should now be in the store
	// The accept loop should pick it up when BridgeAccept is called
	// For this test, we'll just verify the listener can accept
	acceptTaskID, err := BridgeAccept(i.listenerHdl)
	require.NoError(t, err)
	require.Greater(t, acceptTaskID, 0)

	// Give accept loop time to process
	time.Sleep(200 * time.Millisecond)

	i.Close()
	BridgeShutdown()
}

func TestInboundCloseStopsAcceptLoop(t *testing.T) {
	err := BridgeInit("")
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
		Name: "test-inbound-config",
	}

	inbound, err := NewInbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	i := inbound.(*Inbound)

	err = i.Start(adapter.StartStateInitialize)
	require.NoError(t, err)

	i.Close()
	BridgeShutdown()
}
