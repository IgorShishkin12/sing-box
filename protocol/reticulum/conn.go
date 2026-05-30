package reticulum

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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

func newReticulumConn(handle uint64, localName, remoteName string) *reticulumConn {
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
	for {
		n := BridgeRead(c.handle, b)
		if n < 0 {
			return 0, io.EOF // connection closed
		}
		if n > 0 {
			return n, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ReadMessage reads one complete discrete Reticulum message via polling.
// Reticulum preserves message boundaries, so each call returns exactly the
// bytes written by a single BridgeWrite on the remote side.
func (c *reticulumConn) ReadMessage() ([]byte, error) {
	if c.handle == 0 {
		return nil, io.ErrClosedPipe
	}
	buf := make([]byte, 500*1024)
	for {
		n := BridgeRead(c.handle, buf)
		if n < 0 {
			return nil, io.EOF
		}
		if n > 0 {
			msg := buf[:n]
			log.Printf("[reticulum] recv handle=%d len=%d bytes=%x", c.handle, n, msg)
			return msg, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// writeDestHeader sends the proxy destination address as a control message.
func writeDestHeader(io AuthIO, addr string) error {
	if addr == "" {
		return errors.New("empty destination header")
	}
	return io.WriteMsg(addr)
}

// readDestHeader reads the proxy destination address from the control channel.
func readDestHeader(io AuthIO) (string, error) {
	addr, err := io.ReadMsg()
	if err != nil {
		return "", err
	}
	if addr == "" {
		return "", errors.New("empty destination header")
	}
	return addr, nil
}

func (c *reticulumConn) Write(b []byte) (int, error) {
	if c.handle == 0 {
		return 0, io.ErrClosedPipe
	}
	log.Printf("[reticulum] send handle=%d len=%d bytes=%x", c.handle, len(b), b)
	// reticulum_write always writes all bytes or returns -1; partial writes cannot occur.
	n := BridgeWrite(c.handle, b)
	if n < 0 {
		log.Printf("[reticulum] send FAILED handle=%d", c.handle)
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
