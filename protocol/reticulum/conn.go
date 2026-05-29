package reticulum

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// connDataChans maps connID (uint64) → chan []byte.
// Populated by newReticulumConn; written by goOnData; closed by goOnClose.
var connDataChans sync.Map

// globalAcceptCh receives new inbound connection events delivered by the
// Rust bridge's on_accept callback (bridge_stub_reticulum.go → goOnAccept).
// In stub builds (no with_reticulum) it is never written to.
var globalAcceptCh = make(chan acceptEvent, 256)

type acceptEvent struct {
	listenerID uint64
	connID     uint64
	peerHash   string
}

// reticulumConn implements net.Conn using the Reticulum bridge.
//
// Inbound data is delivered via the goOnData callback registered at
// BridgeInit time: the callback pushes bytes into dataCh, which Read
// drains.  Writes go directly to BridgeWrite.
type reticulumConn struct {
	id        uint64
	dataCh    chan []byte // receives data from goOnData
	pending   []byte     // leftover bytes from a partial Read
	closeOnce sync.Once
	closed    chan struct{}

	// synthetic addresses
	localAddr  net.Addr
	remoteAddr net.Addr
}

func newReticulumConn(id uint64, localName, remoteName string) *reticulumConn {
	// Reuse a channel pre-registered by goOnConnect/goOnAccept if present,
	// so that packets arriving in the race window before this call are not lost.
	var ch chan []byte
	if v, ok := connDataChans.Load(id); ok {
		ch = v.(chan []byte)
	} else {
		ch = make(chan []byte, 256)
		connDataChans.Store(id, ch)
	}
	return &reticulumConn{
		id:         id,
		dataCh:     ch,
		closed:     make(chan struct{}),
		localAddr:  reticulumAddr{network: "reticulum", str: localName},
		remoteAddr: reticulumAddr{network: "reticulum", str: remoteName},
	}
}

type reticulumAddr struct {
	network string
	str     string
}

func (a reticulumAddr) Network() string { return a.network }
func (a reticulumAddr) String() string  { return a.str }

// ReadMessage reads one complete discrete message from the Reticulum transport.
// Each call returns exactly the bytes delivered by a single BridgeWrite on the
// remote side, preserving message boundaries.
func (c *reticulumConn) ReadMessage() ([]byte, error) {
	select {
	case chunk, ok := <-c.dataCh:
		if !ok {
			return nil, io.EOF
		}
		return chunk, nil
	case <-c.closed:
		return nil, io.EOF
	}
}

// Read blocks until data arrives (or the connection closes).
func (c *reticulumConn) Read(b []byte) (int, error) {
	for {
		if len(c.pending) > 0 {
			n := copy(b, c.pending)
			c.pending = c.pending[n:]
			return n, nil
		}
		select {
		case chunk, ok := <-c.dataCh:
			if !ok {
				return 0, io.EOF
			}
			n := copy(b, chunk)
			if n < len(chunk) {
				c.pending = chunk[n:]
			}
			return n, nil
		case <-c.closed:
			return 0, io.EOF
		}
	}
}

func (c *reticulumConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	n := BridgeWrite(c.id, b)
	if n < 0 {
		return 0, errors.New("write error")
	}
	return n, nil
}

func (c *reticulumConn) Close() error {
	c.closeOnce.Do(func() {
		connDataChans.Delete(c.id)
		BridgeClose(c.id)
		close(c.closed)
	})
	return nil
}

func (c *reticulumConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *reticulumConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *reticulumConn) SetDeadline(_ time.Time) error      { return nil }
func (c *reticulumConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *reticulumConn) SetWriteDeadline(_ time.Time) error { return nil }

// ---------------------------------------------------------------------------
// Destination header — sent as an AUTH_CTRL message after auth completes.
// ---------------------------------------------------------------------------

// writeDestHeader sends the proxy destination address as a control message.
// Must be called after auth completes (gate is already open on caller's side).
func writeDestHeader(io AuthIO, addr string) error {
	if addr == "" {
		return errors.New("empty destination header")
	}
	return io.WriteMsg(addr)
}

// readDestHeader receives the proxy destination address from the control channel.
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
