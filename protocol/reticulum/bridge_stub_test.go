//go:build with_reticulum

package reticulum

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
		taskID, resultCh, err := BridgeDial(bad)
		require.NoError(t, err, "BridgeDial is async; task creation itself doesn't fail")
		select {
		case connID := <-resultCh:
			require.Zero(t, connID,
				"expected connection to fail (connID=0) for invalid input %q (task %d)", bad, taskID)
		case <-time.After(5 * time.Second):
			t.Fatalf("BridgeDial did not complete within timeout for task %d (input %q)", taskID, bad)
		}
	}

	BridgeShutdown()
}
