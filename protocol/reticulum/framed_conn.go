package reticulum

import (
	"io"
	"net"
	"sync"
	"time"
)

// Message type prefixes. High bit 1 = control, 0 = pass-through data.
const (
	TypeData      byte = 0x00 // pass-through; low 7 bits must be 0
	TypeAuthCtrl  byte = 0x80 // auth exchange message
	TypeReauthReq byte = 0x81 // peer requests re-authentication
)

// AuthIO is the interface used by the auth handshake to exchange binary messages.
type AuthIO interface {
	ReadMsg() ([]byte, error)
	WriteMsg([]byte) error
}

// framedConn wraps a *reticulumConn and demultiplexes framed messages.
//
// A reader goroutine routes incoming messages:
//   - TypeAuthCtrl / TypeReauthReq → ctrlCh (carrying [type][payload...])
//   - TypeData                     → dataCh (buffered; accessible only after OpenGate)
//
// Implements net.Conn and AuthIO. Callers must call OpenGate() after auth.
type framedConn struct {
	inner *reticulumConn

	ctrlCh chan []byte // control messages: [type_byte][payload...]
	dataCh chan []byte // data payloads

	gateOnce  sync.Once
	gate      chan struct{} // closed by OpenGate()
	closeOnce sync.Once
	done      chan struct{} // closed by Close(); interrupts blocked Read()

	leftover []byte // unread tail from last data chunk
}

// newFramedConn wraps raw and starts the demux goroutine.
func newFramedConn(raw *reticulumConn) *framedConn {
	fc := &framedConn{
		inner:  raw,
		ctrlCh: make(chan []byte, 16),
		dataCh: make(chan []byte, 256),
		gate:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go fc.readLoop()
	return fc
}

// OpenGate signals that authentication is complete; Read() may now return data.
func (fc *framedConn) OpenGate() {
	fc.gateOnce.Do(func() { close(fc.gate) })
}

// readLoop demultiplexes framed messages. Does not block on gate; data that
// arrives before auth completes is buffered in dataCh.
func (fc *framedConn) readLoop() {
	defer func() {
		close(fc.ctrlCh)
		close(fc.dataCh)
	}()
	for {
		typ, payload, err := fc.inner.ReadMessage()
		if err != nil {
			return
		}
		if typ == TypeData {
			select {
			case fc.dataCh <- payload:
			case <-fc.done:
				return
			default:
				// Drop oldest to avoid stalling.
				select {
				case <-fc.dataCh:
				default:
				}
				select {
				case fc.dataCh <- payload:
				default:
				}
			}
		} else {
			msg := make([]byte, 1+len(payload))
			msg[0] = typ
			copy(msg[1:], payload)
			select {
			case fc.ctrlCh <- msg:
			case <-fc.done:
				return
			default:
				// Drop control message if channel full.
			}
		}
	}
}

// --- AuthIO ---

// WriteMsg sends a TypeAuthCtrl framed message.
func (fc *framedConn) WriteMsg(msg []byte) error {
	return fc.inner.WriteMessage(TypeAuthCtrl, msg)
}

// ReadMsg returns the payload of the next control message.
func (fc *framedConn) ReadMsg() ([]byte, error) {
	select {
	case msg, ok := <-fc.ctrlCh:
		if !ok {
			return nil, io.EOF
		}
		if len(msg) < 1 {
			return nil, io.EOF
		}
		return msg[1:], nil
	case <-fc.done:
		return nil, io.EOF
	}
}

// --- net.Conn ---

// Read returns data, blocking until gate is open and data is available.
// Returns io.EOF immediately if Close() has been called.
func (fc *framedConn) Read(b []byte) (int, error) {
	if len(fc.leftover) > 0 {
		n := copy(b, fc.leftover)
		fc.leftover = fc.leftover[n:]
		return n, nil
	}
	// Wait for auth or connection close.
	select {
	case <-fc.gate:
	case <-fc.done:
		return 0, io.EOF
	}
	select {
	case data, ok := <-fc.dataCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(b, data)
		if n < len(data) {
			fc.leftover = make([]byte, len(data)-n)
			copy(fc.leftover, data[n:])
		}
		return n, nil
	case <-fc.done:
		return 0, io.EOF
	}
}

// Write sends a TypeData framed message.
func (fc *framedConn) Write(b []byte) (int, error) {
	if err := fc.inner.WriteMessage(TypeData, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close unblocks all blocked Read/ReadMsg calls, then closes the underlying conn.
func (fc *framedConn) Close() error {
	fc.closeOnce.Do(func() {
		close(fc.done)
	})
	fc.OpenGate()
	return fc.inner.Close()
}

func (fc *framedConn) LocalAddr() net.Addr               { return fc.inner.LocalAddr() }
func (fc *framedConn) RemoteAddr() net.Addr              { return fc.inner.RemoteAddr() }
func (fc *framedConn) SetDeadline(t time.Time) error     { return fc.inner.SetDeadline(t) }
func (fc *framedConn) SetReadDeadline(t time.Time) error { return fc.inner.SetReadDeadline(t) }
func (fc *framedConn) SetWriteDeadline(t time.Time) error {
	return fc.inner.SetWriteDeadline(t)
}
