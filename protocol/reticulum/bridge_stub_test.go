//go:build with_reticulum

package reticulum

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBridgePollErrorReturnsMessage(t *testing.T) {
	// Initialize bridge
	err := BridgeInit("{}")
	require.NoError(t, err)

	// Accept on an invalid listener handle (0) — this should fail
	taskID, err := BridgeAccept(0)
	require.NoError(t, err)
	require.Greater(t, taskID, 0)

	// Poll for the result — should return an error with a message
	_, err = BridgePollTask(taskID, 5*time.Second)
	require.Error(t, err)
	// The error should contain the Rust error message, not the generic one
	require.Contains(t, err.Error(), "invalid listener handle")
	require.NotContains(t, err.Error(), "bridge poll failed")

	BridgeShutdown()
}

func TestBridgePollInvalidTaskID(t *testing.T) {
	// Poll with an invalid task ID
	_, err := BridgePollTask(-1, 5*time.Second)
	require.Error(t, err)
}

// TestBridgeDialFailsWithNonHashString is a regression test for the Android crash
// where the TCP proxy destination ("host:port") was passed to BridgeDial instead
// of the Reticulum destination hash. parse_dest_hash rejects non-hex strings.
func TestBridgeDialFailsWithNonHashString(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	// These are the kinds of strings that would be incorrectly passed to BridgeDial
	// if the caller confused the proxied TCP address with the Reticulum destination.
	invalidInputs := []string{
		"example.com:443",
		"127.0.0.1:8080",
		"not-a-hash",
	}

	for _, bad := range invalidInputs {
		taskID, err := BridgeDial(bad)
		require.NoError(t, err, "BridgeDial is async; task creation itself doesn't fail")
		_, err = BridgePollTask(taskID, 5*time.Second)
		require.Error(t, err, "polling task for %q must fail: it is not a valid hex hash", bad)
		require.Contains(t, err.Error(), "invalid destination hash hex string",
			"error should name the specific parse failure for %q", bad)
	}

	BridgeShutdown()
}
