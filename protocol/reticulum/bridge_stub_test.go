//go:build with_reticulum

package reticulum

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBridgePollErrorReturnsMessage(t *testing.T) {
	// Initialize bridge
	err := BridgeInit("")
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