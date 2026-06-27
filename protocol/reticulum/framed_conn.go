package reticulum

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// packetReader is implemented by connections that deliver complete messages
// without size limits (as opposed to the net.Conn byte-stream Read interface).
// framedConn.readLoop and muxSession.readLoop use it to preserve message
// boundaries for large Resource-delivered payloads.
type packetReader interface {
	ReadPacket() ([]byte, error)
}

// AuthIO sends and receives binary auth messages over typed control channels.
// The type byte distinguishes round 1 (TypeRequestAuth) from round 2 (TypeResponseAuth),
// so either side can detect and reject messages that arrive out of sequence.
type AuthIO interface {
	ReadMsg() (typeByte byte, payload []byte, err error)
	WriteMsg(typeByte byte, payload []byte) error
}

// authInactivityTimeout is the max idle time ReadMsg waits for the peer's NEXT auth
// message after we have already received at least one correct response on this
// connection. Once the peer has proved it is alive, a shorter window catches
// silent failures faster — e.g. a half-open TCP connection or a peer that died
// between auth rounds. On congested LoRa channels each packet can take ~5–10 s to
// deliver; 20 s leaves a comfortable margin while still being much shorter than
// the 30 s global authTimeout used when the peer has never responded.
const authInactivityTimeout = 20 * time.Second

// framedConn wraps a message-boundary net.Conn and demultiplexes by type byte.
//
// TypeRequestAuth (0x84) and TypeResponseAuth (0x85) messages go to the auth
// channel and are returned by ReadMsg (full message including type byte).
// All other messages are buffered in dataCh until OpenGate is called, after
// which they are returned by Read (used by the mux layer above).
//
// This means auth and data are never confused even if both arrive on the same link,
// and the mux cannot start consuming data before auth completes.
type framedConn struct {
	inner    net.Conn
	ctrlCh   chan []byte   // full auth messages (type byte + payload)
	dataCh   chan []byte   // full raw messages for all non-auth messages
	gate     chan struct{} // closed by OpenGate; Read blocks until then
	gateOnce sync.Once
	done     chan struct{} // closed by Close
	doneOnce sync.Once
	readBuf  []byte // leftover bytes from last dataCh receive

	// lastCtrlAt records when the readLoop last routed an auth message to ctrlCh
	// (unix nanoseconds; 0 = never). Used by ReadMsg to apply a tighter inactivity
	// deadline once the peer has demonstrated it is alive.
	lastCtrlAt atomic.Int64

	// inactivityTimeout overrides authInactivityTimeout when non-zero. Set in tests.
	inactivityTimeout time.Duration

	localAddr  net.Addr
	remoteAddr net.Addr
}

// newFramedConn wraps inner and starts the demux read loop.
// inner must have message-boundary Read semantics (each Read returns one complete message).
func newFramedConn(inner net.Conn) *framedConn {
	fc := &framedConn{
		inner:      inner,
		ctrlCh:     make(chan []byte, 8),
		dataCh:     make(chan []byte, 128),
		gate:       make(chan struct{}),
		done:       make(chan struct{}),
		localAddr:  inner.LocalAddr(),
		remoteAddr: inner.RemoteAddr(),
	}
	go fc.readLoop()
	return fc
}

// readLoop reads messages from inner and routes them by type byte.
// Runs until the underlying connection closes or framedConn is closed.
//
// When inner implements packetReader, each ReadPacket() call returns a complete
// message of arbitrary size (used for large Resource-delivered payloads).
// Otherwise falls back to fixed-size Read for byte-stream connections (tests).
func (fc *framedConn) readLoop() {
	defer close(fc.dataCh) // signals EOF to Read

	route := func(msg []byte) bool {
		if msg[0] == TypeRequestAuth || msg[0] == TypeResponseAuth {
			fc.lastCtrlAt.Store(time.Now().UnixNano())
			select {
			case fc.ctrlCh <- msg:
			case <-fc.done:
				return false
			}
		} else {
			select {
			case fc.dataCh <- msg:
			case <-fc.done:
				return false
			}
		}
		return true
	}

	if pr, ok := fc.inner.(packetReader); ok {
		for {
			msg, err := pr.ReadPacket()
			if err != nil {
				return
			}
			if len(msg) == 0 {
				continue
			}
			if !route(msg) {
				return
			}
		}
	} else {
		buf := make([]byte, MaxReticulumMessage)
		for {
			n, err := fc.inner.Read(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			msg := make([]byte, n)
			copy(msg, buf[:n])
			if !route(msg) {
				return
			}
		}
	}
}

// OpenGate allows Read to start returning data messages.
// Must be called after auth completes (or when no auth is configured).
func (fc *framedConn) OpenGate() {
	fc.gateOnce.Do(func() { close(fc.gate) })
}

// ReadPacket returns one complete data message without any size limit.
// Blocks until OpenGate has been called, then returns the next message from
// dataCh as a single contiguous slice. Large Resource-delivered payloads
// arrive whole, unlike Read which may split them across multiple calls.
func (fc *framedConn) ReadPacket() ([]byte, error) {
	select {
	case <-fc.gate:
	case <-fc.done:
		return nil, io.ErrClosedPipe
	}
	select {
	case msg, ok := <-fc.dataCh:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-fc.done:
		select {
		case msg, ok := <-fc.dataCh:
			if !ok {
				return nil, io.EOF
			}
			return msg, nil
		default:
		}
		return nil, io.ErrClosedPipe
	}
}

// Read implements net.Conn. Blocks until OpenGate is called, then returns
// the next data message. Returns io.EOF when the underlying connection closes.
func (fc *framedConn) Read(b []byte) (int, error) {
	if len(fc.readBuf) > 0 {
		n := copy(b, fc.readBuf)
		fc.readBuf = fc.readBuf[n:]
		return n, nil
	}

	select {
	case <-fc.gate:
	case <-fc.done:
		return 0, io.ErrClosedPipe
	}

	select {
	case msg, ok := <-fc.dataCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(b, msg)
		if n < len(msg) {
			fc.readBuf = msg[n:]
		}
		return n, nil
	case <-fc.done:
		return 0, io.ErrClosedPipe
	}
}

// Write implements net.Conn. Passes bytes directly to inner (mux packets already
// carry their own type byte as the first byte).
func (fc *framedConn) Write(b []byte) (int, error) {
	return fc.inner.Write(b)
}

// ReadMsg reads one auth control message. Returns the type byte (TypeRequestAuth or
// TypeResponseAuth) and payload.
//
// Two deadlines apply:
//   - Global: authTimeout from now (used whenever the peer has never yet responded).
//   - Inactivity: authInactivityTimeout from when the peer last sent a correct auth
//     message. Once the peer has proved it is alive, a tighter window catches silent
//     failures faster without impacting the first-contact case.
//
// The effective deadline is whichever is sooner.
func (fc *framedConn) ReadMsg() (byte, []byte, error) {
	inactTimeout := fc.inactivityTimeout
	if inactTimeout == 0 {
		inactTimeout = authInactivityTimeout
	}
	deadline := time.Now().Add(authTimeout)
	if lastNano := fc.lastCtrlAt.Load(); lastNano != 0 {
		if inact := time.Unix(0, lastNano).Add(inactTimeout); inact.Before(deadline) {
			deadline = inact
		}
	}
	select {
	case msg, ok := <-fc.ctrlCh:
		if !ok {
			return 0, nil, io.EOF
		}
		return msg[0], msg[1:], nil
	case <-fc.done:
		return 0, nil, io.ErrClosedPipe
	case <-time.After(time.Until(deadline)):
		return 0, nil, fmt.Errorf("auth timeout")
	}
}

// WriteMsg sends payload as an auth control message with the given type byte
// (TypeRequestAuth for round 1, TypeResponseAuth for round 2).
func (fc *framedConn) WriteMsg(typeByte byte, payload []byte) error {
	msg := make([]byte, 1+len(payload))
	msg[0] = typeByte
	copy(msg[1:], payload)
	_, err := fc.inner.Write(msg)
	return err
}

// Close shuts down the framedConn and the underlying connection.
func (fc *framedConn) Close() error {
	fc.doneOnce.Do(func() { close(fc.done) })
	return fc.inner.Close()
}

func (fc *framedConn) LocalAddr() net.Addr                { return fc.localAddr }
func (fc *framedConn) RemoteAddr() net.Addr               { return fc.remoteAddr }
func (fc *framedConn) SetDeadline(t time.Time) error      { return fc.inner.SetDeadline(t) }
func (fc *framedConn) SetReadDeadline(t time.Time) error  { return fc.inner.SetReadDeadline(t) }
func (fc *framedConn) SetWriteDeadline(t time.Time) error { return fc.inner.SetWriteDeadline(t) }
