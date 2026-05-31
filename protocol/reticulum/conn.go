package reticulum

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/log"
)

// connEntry holds the per-connection async state.
// dataCh carries inbound packets (one Reticulum packet per send).
// done is closed when the connection is torn down from either side.
type connEntry struct {
	ch   chan []byte
	done chan struct{}
	once sync.Once
}

// connDataChans maps conn_id → *connEntry for the async callback model.
// Populated by newReticulumConn, consumed/closed by goOnData/goOnClose.
var connDataChans sync.Map // uint64 → *connEntry

// acceptEvent is delivered via globalAcceptCh when Rust fires on_accept.
type acceptEvent struct {
	listenerID uint64
	connID     uint64
	peerHash   string
}

// globalAcceptCh receives inbound connection events from the Rust on_accept callback.
var globalAcceptCh = make(chan acceptEvent, 256)

// pendingDials maps task_id → chan uint64 for the on_connect callback.
var pendingDials sync.Map // uint64 → chan uint64

// nextDialTask is the monotonically increasing task ID counter for dials.
var nextDialTask sync.Mutex // protects nextDialTaskSeq
var nextDialTaskSeq uint64

type reticulumAddr struct {
	network string
	str     string
}

func (a reticulumAddr) Network() string { return a.network }
func (a reticulumAddr) String() string  { return a.str }

// reticulumConn implements net.Conn using the Reticulum bridge.
// Read blocks until a packet arrives via the on_data callback (no polling).
type reticulumConn struct {
	handle     uint64
	entry      *connEntry
	pending    []byte // leftover bytes from last dataCh receive
	localAddr  net.Addr
	remoteAddr net.Addr
	logger     log.ContextLogger
}

func newReticulumConn(handle uint64, localName, remoteName string, logger log.ContextLogger) *reticulumConn {
	// Reuse any entry pre-created by goOnData (data may arrive before this call
	// when the Rust data reader starts ahead of the Go accept-loop goroutine).
	actual, _ := connDataChans.LoadOrStore(handle, &connEntry{
		ch:   make(chan []byte, 256),
		done: make(chan struct{}),
	})
	entry := actual.(*connEntry)
	return &reticulumConn{
		handle:     handle,
		entry:      entry,
		localAddr:  reticulumAddr{network: "reticulum", str: localName},
		remoteAddr: reticulumAddr{network: "reticulum", str: remoteName},
		logger:     logger,
	}
}

// Read implements net.Conn. Blocks until a full Reticulum packet arrives.
// Each call returns exactly one packet (message-boundary semantics required
// by framedConn's reader goroutine).
func (c *reticulumConn) Read(b []byte) (int, error) {
	if c.handle == 0 {
		return 0, io.ErrClosedPipe
	}
	// Serve any leftover bytes from the previous packet first.
	if len(c.pending) > 0 {
		n := copy(b, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	select {
	case chunk := <-c.entry.ch:
		if c.logger != nil {
			c.logger.Trace("reticulumConn.Read: handle=", c.handle, " n=", len(chunk), " data=", fmt.Sprintf("%q", chunk))
		}
		n := copy(b, chunk)
		if n < len(chunk) {
			c.pending = make([]byte, len(chunk)-n)
			copy(c.pending, chunk[n:])
		}
		return n, nil
	case <-c.entry.done:
		// Drain any packets that arrived just before the close signal.
		select {
		case chunk := <-c.entry.ch:
			n := copy(b, chunk)
			if n < len(chunk) {
				c.pending = make([]byte, len(chunk)-n)
				copy(c.pending, chunk[n:])
			}
			return n, nil
		default:
		}
		if c.logger != nil {
			c.logger.Trace("reticulumConn.Read: handle=", c.handle, " EOF")
		}
		return 0, io.EOF
	}
}

// writeDestHeader writes a 2-byte big-endian length followed by the address string.
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
	if c.logger != nil {
		c.logger.Trace("BridgeWrite: handle=", c.handle, " len=", len(b), " data=", fmt.Sprintf("%q", b))
	}
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
	handle := c.handle
	c.handle = 0
	connDataChans.Delete(handle)
	c.entry.once.Do(func() { close(c.entry.done) })
	BridgeClose(handle)
	return nil
}

func (c *reticulumConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *reticulumConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *reticulumConn) SetDeadline(_ time.Time) error      { return nil }
func (c *reticulumConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *reticulumConn) SetWriteDeadline(_ time.Time) error { return nil }
