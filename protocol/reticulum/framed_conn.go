package reticulum

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// AuthIO sends and receives binary auth messages over typed control channels.
// The type byte distinguishes round 1 (TypeRequestAuth) from round 2 (TypeResponseAuth),
// so either side can detect and reject messages that arrive out of sequence.
type AuthIO interface {
	ReadMsg() (typeByte byte, payload []byte, err error)
	WriteMsg(typeByte byte, payload []byte) error
}

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
func (fc *framedConn) readLoop() {
	defer close(fc.dataCh) // signals EOF to Read
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

		if msg[0] == TypeRequestAuth || msg[0] == TypeResponseAuth {
			select {
			case fc.ctrlCh <- msg: // include type byte so ReadMsg can verify sequence
			case <-fc.done:
				return
			}
		} else {
			select {
			case fc.dataCh <- msg:
			case <-fc.done:
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
// TypeResponseAuth) and payload. Blocks with authTimeout.
func (fc *framedConn) ReadMsg() (byte, []byte, error) {
	select {
	case msg, ok := <-fc.ctrlCh:
		if !ok {
			return 0, nil, io.EOF
		}
		return msg[0], msg[1:], nil
	case <-fc.done:
		return 0, nil, io.ErrClosedPipe
	case <-time.After(authTimeout):
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
