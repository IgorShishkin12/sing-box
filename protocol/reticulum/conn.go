package reticulum

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// BridgePollTask polls for task completion with a timeout.
// Returns the handle (8 bytes LE) on success, or an error.
func BridgePollTask(taskID int, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for {
		done, result, err := BridgePoll(taskID)
		if err != nil {
			return 0, err
		}
		if done {
			if len(result) != 8 {
				return 0, fmt.Errorf("unexpected result length: %d", len(result))
			}
			return uint64(result[0]) | uint64(result[1])<<8 | uint64(result[2])<<16 | uint64(result[3])<<24 |
				uint64(result[4])<<32 | uint64(result[5])<<40 | uint64(result[6])<<48 | uint64(result[7])<<56, nil
		}
		if time.Now().After(deadline) {
			return 0, context.DeadlineExceeded
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type reticulumAddr struct {
	network string
	str     string
}

func (a reticulumAddr) Network() string { return a.network }
func (a reticulumAddr) String() string  { return a.str }

// reticulumConn implements net.Conn using the Reticulum bridge.
type reticulumConn struct {
	handle uint64
	// local/remote addresses are synthetic since Reticulum is destination-based
	localAddr  net.Addr
	remoteAddr net.Addr
}

func newReticulumConn(handle uint64, localName, remoteName string) net.Conn {
	return &reticulumConn{
		handle:     handle,
		localAddr:  reticulumAddr{network: "reticulum", str: localName},
		remoteAddr: reticulumAddr{network: "reticulum", str: remoteName},
	}
}

func (c *reticulumConn) Read(b []byte) (int, error) {
	if c.handle == 0 {
		return 0, io.ErrClosedPipe
	}
	n := BridgeRead(c.handle, b)
	if n < 0 {
		return 0, errors.New("read error")
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (c *reticulumConn) Write(b []byte) (int, error) {
	if c.handle == 0 {
		return 0, io.ErrClosedPipe
	}
	// reticulum_write always writes all bytes or returns -1; partial writes cannot occur.
	n := BridgeWrite(c.handle, b)
	if n < 0 {
		return 0, errors.New("write error")
	}
	return n, nil
}

func (c *reticulumConn) Close() error {
	if c.handle == 0 {
		return nil
	}
	BridgeClose(c.handle)
	c.handle = 0
	return nil
}

func (c *reticulumConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *reticulumConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *reticulumConn) SetDeadline(t time.Time) error {
	// Not implemented in the stub; return nil to avoid breaking callers.
	return nil
}

func (c *reticulumConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *reticulumConn) SetWriteDeadline(t time.Time) error {
	return nil
}
