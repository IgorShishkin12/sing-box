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

func TestBridgeReadOnUnknownHandleReturnsError(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	// Any handle not in the store returns -1
	buf := make([]byte, 10)
	n := BridgeRead(99999, buf)
	require.Equal(t, -1, n)

	BridgeShutdown()
}
