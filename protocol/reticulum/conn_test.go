//go:build with_reticulum

package reticulum

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReticulumConnReadWrite(t *testing.T) {
	// Initialize bridge
	err := BridgeInit("")
	require.NoError(t, err)

	// Dial a destination to get a connection
	taskID, err := BridgeDial("rln://test")
	require.NoError(t, err)
	require.Greater(t, taskID, 0)

	// Poll for completion
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)
	require.Greater(t, handle, uint64(0))

	c := newReticulumConn(handle, "local", "remote").(*reticulumConn)

	// Write data
	data := []byte("hello world")
	n, err := c.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// Read it back (the in-memory connection echoes writes to its read buffer)
	buf := make([]byte, len(data))
	n, err = c.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)

	// EOF after draining
	buf2 := make([]byte, 1)
	n, err = c.Read(buf2)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	c.Close()
	BridgeShutdown()
}

func TestReticulumConnClose(t *testing.T) {
	err := BridgeInit("")
	require.NoError(t, err)

	taskID, err := BridgeDial("rln://test2")
	require.NoError(t, err)
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)

	c := newReticulumConn(handle, "local", "remote").(*reticulumConn)

	// Close once
	err = c.Close()
	require.NoError(t, err)

	// Subsequent Close should be no-op
	err = c.Close()
	require.NoError(t, err)

	// Read/Write after close should return ErrClosedPipe
	_, err = c.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe)

	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.ErrClosedPipe)

	BridgeShutdown()
}

func TestReticulumConnDeadlines(t *testing.T) {
	err := BridgeInit("")
	require.NoError(t, err)

	taskID, err := BridgeDial("rln://test3")
	require.NoError(t, err)
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)

	c := newReticulumConn(handle, "local", "remote").(*reticulumConn)

	// These should not panic. The stub returns nil, but we just ensure calls are safe.
	now := time.Now()
	require.NoError(t, c.SetDeadline(now.Add(time.Second)))
	require.NoError(t, c.SetReadDeadline(now.Add(time.Second)))
	require.NoError(t, c.SetWriteDeadline(now.Add(time.Second)))

	c.Close()
	BridgeShutdown()
}

func TestReticulumConnAddresses(t *testing.T) {
	err := BridgeInit("")
	require.NoError(t, err)

	taskID, err := BridgeDial("rln://test4")
	require.NoError(t, err)
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)

	c := newReticulumConn(handle, "local", "remote").(*reticulumConn)

	local := c.LocalAddr()
	remote := c.RemoteAddr()
	require.NotNil(t, local)
	require.NotNil(t, remote)
	require.Equal(t, "reticulum", local.Network())
	require.Equal(t, "reticulum", remote.Network())

	c.Close()
	BridgeShutdown()
}

func TestReticulumConnConcurrentClose(t *testing.T) {
	err := BridgeInit("")
	require.NoError(t, err)

	taskID, err := BridgeDial("rln://test5")
	require.NoError(t, err)
	handle, err := BridgePollTask(taskID, 5*time.Second)
	require.NoError(t, err)

	c := newReticulumConn(handle, "local", "remote").(*reticulumConn)

	done := make(chan error, 2)
	go func() { done <- c.Close() }()
	go func() { done <- c.Close() }()

	// Both should succeed (second is no-op)
	require.NoError(t, <-done)
	require.NoError(t, <-done)

	// Final state: handle zeroed
	require.Equal(t, uint64(0), c.handle)

	BridgeShutdown()
}
