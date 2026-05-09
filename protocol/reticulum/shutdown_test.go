//go:build with_reticulum

package reticulum

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBridgeShutdownCleanly(t *testing.T) {
	// Init → shutdown should not panic
	err := BridgeInit("")
	require.NoError(t, err)
	BridgeShutdown()
}

func TestBridgeShutdownWithoutInit(t *testing.T) {
	// Shutdown without init should not panic
	BridgeShutdown()
}

func TestBridgeInitShutdownInit(t *testing.T) {
	// Init → shutdown → re-init should work
	err := BridgeInit("")
	require.NoError(t, err)
	BridgeShutdown()

	err = BridgeInit("")
	require.NoError(t, err)
	BridgeShutdown()
}

func TestBridgeShutdownRemovesHandles(t *testing.T) {
	// Init
	err := BridgeInit("")
	require.NoError(t, err)

	// Create a connection
	taskID, err := BridgeDial("rln://test-shutdown")
	require.NoError(t, err)
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)
	require.Greater(t, handle, uint64(0))

	// Shutdown
	BridgeShutdown()

	// Re-init
	err = BridgeInit("")
	require.NoError(t, err)

	// Attempt to read from old handle — should fail since store was cleared
	buf := make([]byte, 10)
	n := BridgeRead(handle, buf)
	require.Equal(t, -1, n, "reading from old handle after re-init should fail")

	BridgeShutdown()
}

func TestBridgeShutdownMultiHandle(t *testing.T) {
	// Init and create resources
	err := BridgeInit("")
	require.NoError(t, err)

	// Create a listener
	listenTaskID, err := BridgeListen("multi-test-hash")
	require.NoError(t, err)
	listenerHdl, err := BridgePollTask(listenTaskID, 5*time.Second)
	require.NoError(t, err)

	// Create a connection
	dialTaskID, err := BridgeDial("multi-test-hash")
	require.NoError(t, err)
	dialHdl, err := BridgePollTask(dialTaskID, 5*time.Second)
	require.NoError(t, err)

	// Shutdown drops all handles
	BridgeShutdown()

	// Re-init
	err = BridgeInit("")
	require.NoError(t, err)

	// Old handles should be invalid
	buf := make([]byte, 10)
	n := BridgeRead(dialHdl, buf)
	require.Equal(t, -1, n, "old handle should be invalid after re-init")

	// Accept on old listener should fail
	taskID, err := BridgeAccept(listenerHdl)
	require.NoError(t, err)
	_, err = BridgePollTask(taskID, 5*time.Second)
	require.Error(t, err, "accept on old listener should fail")

	BridgeShutdown()
}