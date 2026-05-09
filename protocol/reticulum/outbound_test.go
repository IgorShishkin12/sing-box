//go:build with_reticulum

package reticulum

import (
	"context"
	"os"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestOutboundDialContextReturnsConn(t *testing.T) {
	// Initialize bridge
	err := BridgeInit("")
	require.NoError(t, err)

	ctx := context.Background()
	var router adapter.Router // nil is fine for this test
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumOutboundOptions{
		Name: "test-outbound",
	}

	outbound, err := NewOutbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	require.NotNil(t, outbound)

	// Type assert to *Outbound to access Start/Close
	o := outbound.(*Outbound)

	// Start the outbound (initializes bridge if needed)
	err = o.Start(adapter.StartStateInitialize)
	require.NoError(t, err)

	// Dial a destination
	dest := M.ParseSocksaddr("127.0.0.1:8080")
	conn, err := o.DialContext(ctx, "tcp", dest)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify it's a reticulumConn
	_, ok := conn.(*reticulumConn)
	require.True(t, ok)

	conn.Close()
	o.Close()
	BridgeShutdown()
}

func TestOutboundStartWithConfigPath(t *testing.T) {
	ctx := context.Background()

	var router adapter.Router
	logger := log.NewNOPFactory().Logger()

	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"
	configContent := `{"identity_path": "/tmp/test", "storage_path": "/tmp/test"}`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	opts := option.ReticulumOutboundOptions{
		ReticulumConfigPath: configPath,
		Name: "test-config-path",
	}

	outbound, err := NewOutbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	o := outbound.(*Outbound)

	err = o.Start(adapter.StartStateInitialize)
	require.NoError(t, err)

	o.Close()
	BridgeShutdown()
}

func TestOutboundDependencies(t *testing.T) {
	ctx := context.Background()

	var router adapter.Router
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumOutboundOptions{}

	outbound, err := NewOutbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	o := outbound.(*Outbound)

	deps := o.Dependencies()
	require.Nil(t, deps)
}

func TestOutboundClose(t *testing.T) {
	err := BridgeInit("")
	require.NoError(t, err)

	ctx := context.Background()
	var router adapter.Router
	logger := log.NewNOPFactory().Logger()
	opts := option.ReticulumOutboundOptions{}

	outbound, err := NewOutbound(ctx, router, logger, "test", opts)
	require.NoError(t, err)
	o := outbound.(*Outbound)

	err = o.Start(adapter.StartStateInitialize)
	require.NoError(t, err)

	// Close should not error
	err = o.Close()
	require.NoError(t, err)

	BridgeShutdown()
}
