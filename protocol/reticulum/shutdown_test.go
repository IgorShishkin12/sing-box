//go:build with_reticulum

package reticulum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBridgeShutdownCleanly(t *testing.T) {
	// Init → shutdown should not panic
	err := BridgeInit("{}")
	require.NoError(t, err)
	BridgeShutdown()
}

func TestBridgeShutdownWithoutInit(t *testing.T) {
	// Shutdown without init should not panic
	BridgeShutdown()
}

func TestBridgeInitShutdownInit(t *testing.T) {
	// Init → shutdown → re-init should work
	err := BridgeInit("{}")
	require.NoError(t, err)
	BridgeShutdown()

	err = BridgeInit("{}")
	require.NoError(t, err)
	BridgeShutdown()
}
