package reticulum

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// maxMsgPayload is the maximum payload length for a framed message.
// Format: [1 byte type][2 byte length BE][payload]. Max payload = 65535.
const maxMsgPayload = 65535

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

// writeDestHeader writes a 2-byte big-endian length followed by the address string.
// Format: uint16 length + UTF-8 "host:port".
func writeDestHeader(w io.Writer, addr string) error {
	b := []byte(addr)
	hdr := make([]byte, 2)
	binary.BigEndian.PutUint16(hdr, uint16(len(b)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// readDestHeader reads a 2-byte big-endian length then the address string.
func readDestHeader(r io.Reader) (string, error) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return "", err
	}
	n := int(binary.BigEndian.Uint16(hdr))
	if n == 0 {
		return "", errors.New("empty destination header")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
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

// WriteMessage sends a framed message: [type][uint16 len BE][payload].
// Returns an error if payload exceeds maxMsgPayload.
func (c *reticulumConn) WriteMessage(typ byte, payload []byte) error {
	if c.handle == 0 {
		return io.ErrClosedPipe
	}
	if len(payload) > maxMsgPayload {
		return fmt.Errorf("reticulum: message payload %d bytes exceeds max %d", len(payload), maxMsgPayload)
	}
	msg := make([]byte, 3+len(payload))
	msg[0] = typ
	binary.BigEndian.PutUint16(msg[1:3], uint16(len(payload)))
	copy(msg[3:], payload)
	if n := BridgeWrite(c.handle, msg); n < 0 {
		return errors.New("write error")
	}
	return nil
}

// ReadMessage reads one framed message: [type][uint16 len BE][payload].
func (c *reticulumConn) ReadMessage() (typ byte, payload []byte, err error) {
	hdr := make([]byte, 3)
	if _, err = io.ReadFull(c, hdr); err != nil {
		return 0, nil, err
	}
	typ = hdr[0]
	payloadLen := int(binary.BigEndian.Uint16(hdr[1:3]))
	if payloadLen == 0 {
		return typ, nil, nil
	}
	payload = make([]byte, payloadLen)
	if _, err = io.ReadFull(c, payload); err != nil {
		return 0, nil, err
	}
	return typ, payload, nil
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
