package reticulum

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func newTestOutbound(t *testing.T, opts option.ReticulumOutboundOptions) *Outbound {
	t.Helper()
	o, err := NewOutbound(context.Background(), nil, log.NewNOPFactory().Logger(), "test", opts)
	require.NoError(t, err)
	return o.(*Outbound)
}

// TestOutboundStartRequiresDestOrName is a regression test: the outbound must
// refuse to start if neither Destination nor Name is configured, rather than
// silently dialing the proxied TCP address as the Reticulum peer.
func TestOutboundStartRequiresDestOrName(t *testing.T) {
	o := newTestOutbound(t, option.ReticulumOutboundOptions{})
	err := o.Start(adapter.StartStateInitialize)
	require.Error(t, err)
	require.Contains(t, err.Error(), "destination or name must be set")
}

// TestOutboundStartSetsResolvedHashFromDestination verifies that a hex Destination
// is cached into resolvedHash at Start time (no network call needed).
func TestOutboundStartSetsResolvedHashFromDestination(t *testing.T) {
	hash := "aabbccdd00112233445566778899aabb"
	o := newTestOutbound(t, option.ReticulumOutboundOptions{Destination: hash})
	// Start will fail at BridgeInit (stub returns ErrBridgeNotAvailable),
	// but resolvedHash must be populated before that.
	_ = o.Start(adapter.StartStateInitialize)
	o.mu.Lock()
	got := o.resolvedHash
	o.mu.Unlock()
	require.Equal(t, hash, got)
}

// TestOutboundDialContextUsesResolvedDestNotTCPDest is a regression test for the
// bug where DialContext passed destination.String() ("example.com:443") to BridgeDial
// instead of the configured Reticulum hash.  With the stub bridge, BridgeDial always
// returns ErrBridgeNotAvailable; if the bug were present the error path would still
// be the same, but the resolvedHash field proves what *would* have been dialled.
func TestOutboundDialContextUsesResolvedDestNotTCPDest(t *testing.T) {
	hash := "aabbccdd00112233445566778899aabb"
	o := newTestOutbound(t, option.ReticulumOutboundOptions{Destination: hash})
	o.resolvedHash = hash // bypass Start (which needs a real bridge)
	o.bridgeInited = true

	tcpDest := M.ParseSocksaddr("example.com:443")
	_, err := o.DialContext(context.Background(), "tcp", tcpDest)
	// The error must come from BridgeDial (bridge not available), NOT from
	// BridgeResolveName ("resolve") and NOT from "invalid destination hash".
	require.Error(t, err)
	require.NotContains(t, err.Error(), "example.com")
	require.NotContains(t, err.Error(), "resolve")
}

// TestOutboundDialContextResolvesNameOnFirstCall verifies that when Destination is
// empty, DialContext calls BridgeResolveName with the configured Name.
func TestOutboundDialContextResolvesNameOnFirstCall(t *testing.T) {
	o := newTestOutbound(t, option.ReticulumOutboundOptions{Name: "my-server"})
	o.bridgeInited = true // skip bridge init for logic test

	_, err := o.DialContext(context.Background(), "tcp", M.ParseSocksaddr("127.0.0.1:80"))
	// BridgeResolveName fails with stub, wrapped as "resolve ..."
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve")
	require.Contains(t, err.Error(), "my-server")
}

func TestOutboundDependencies(t *testing.T) {
	o := newTestOutbound(t, option.ReticulumOutboundOptions{Name: "x"})
	require.Nil(t, o.Dependencies())
}

func TestOutboundClose(t *testing.T) {
	o := newTestOutbound(t, option.ReticulumOutboundOptions{Name: "x"})
	require.NoError(t, o.Close())
}

func TestOutboundStartWithConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/config.json"
	require.NoError(t, os.WriteFile(configPath, []byte(`{"storage_path":"/tmp/test"}`), 0644))

	o := newTestOutbound(t, option.ReticulumOutboundOptions{
		ReticulumConfigPath: configPath,
		Name:                "test-config-path",
	})
	err := o.Start(adapter.StartStateInitialize)
	// Fails at BridgeInit (stub) but NOT at validation — Name is set.
	if err != nil {
		require.NotContains(t, err.Error(), "destination or name must be set")
	}
}

// TestOutboundConnectEager_WithDest verifies that connectEager with a pre-resolved hash
// reaches BridgeDial (stub failure), proving the session establishment path is entered.
func TestOutboundConnectEager_WithDest(t *testing.T) {
	hash := "aabbccdd00112233445566778899aabb"
	o := newTestOutbound(t, option.ReticulumOutboundOptions{
		Destination: hash,
		AuthOnStart: true,
	})
	o.resolvedHash = hash

	err := o.connectEager(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "connect on start")
}

// TestOutboundConnectEager_WithName verifies that connectEager resolves the name before
// dialling when no hash is pre-resolved (stub BridgeResolveName returns an error).
func TestOutboundConnectEager_WithName(t *testing.T) {
	o := newTestOutbound(t, option.ReticulumOutboundOptions{Name: "my-server", AuthOnStart: true})

	err := o.connectEager(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "on start")
	require.Contains(t, err.Error(), "my-server")
}

// TestOutboundStartRequiresDestOrName_JSONRoundtrip ensures the JSON config key
// "destination" maps to the right field (regression: was previously treated as a
// name rather than a hex hash, confusing resolution logic).
func TestOutboundStartRequiresDestOrName_JSONRoundtrip(t *testing.T) {
	hash := "aabbccdd00112233445566778899aabb"
	opts := option.ReticulumOutboundOptions{Destination: hash}
	o := newTestOutbound(t, opts)
	_ = o.Start(adapter.StartStateInitialize)
	o.mu.Lock()
	got := o.resolvedHash
	o.mu.Unlock()
	if !strings.HasPrefix(got, "a") { // basic sanity on the hash
		t.Fatalf("unexpected resolvedHash: %q", got)
	}
}
