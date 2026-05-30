package reticulum

// framed_conn.go — type-prefixed message framing on top of reticulumConn.
//
// Every reticulum message is a discrete packet (BridgeWrite preserves boundaries).
// We prefix each message with exactly one type byte:
//
//	0b0xxxxxxx  DATA      — user traffic
//	0b10000000  AUTH_CTRL — auth handshake + dest header
//	0b10000001  REAUTH_REQ — server→client: re-authenticate (reserved)
//
// During auth the gate is closed: DATA frames are buffered (up to maxDataQueue).
// Once OpenGate() is called, buffered frames are released to Read() in FIFO order.

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	typeMask      byte = 0b10000000
	typeData      byte = 0b00000000
	typeAuthCtrl  byte = 0b10000000 // 0x80
	typeReauthReq byte = 0b10000001 // 0x81

	maxDataQueue = 256
	maxCtrlQueue = 32

	// maxDataPayload is the largest plaintext chunk per Reticulum packet.
	// PACKET_MDU=464, Fernet overhead=48, AES padding up to 16 → max plaintext 400.
	// Subtract 1 byte for the type prefix → 399.
	maxDataPayload = 199
)

// AuthIO is the transport interface used by ServerAuth / ClientAuth.
// Each WriteMsg / ReadMsg corresponds to one discrete protocol message.
type AuthIO interface {
	WriteMsg(text string) error
	ReadMsg() (string, error)
}

// framedConn wraps a *reticulumConn and implements net.Conn + AuthIO.
type framedConn struct {
	raw       *reticulumConn
	ctrlCh    chan []byte
	dataCh    chan []byte
	authDone  chan struct{}
	openOnce  sync.Once
	closed    chan struct{}
	closeOnce sync.Once
}

func newFramedConn(raw *reticulumConn) *framedConn {
	return &framedConn{
		raw:      raw,
		ctrlCh:   make(chan []byte, maxCtrlQueue),
		dataCh:   make(chan []byte, maxDataQueue),
		authDone: make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

// dispatch reads raw messages and routes them by type byte. Must run in its own goroutine.
func (fc *framedConn) dispatch() {
	for {
		msg, err := fc.raw.ReadMessage()
		if err != nil {
			fc.close()
			return
		}
		if len(msg) < 1 {
			continue
		}
		typeByte := msg[0]
		payload := msg[1:]

		if typeByte&typeMask == typeData {
			select {
			case fc.dataCh <- payload:
			default:
				// Queue full — drop frame (acceptable during auth buffering)
			}
		} else if typeByte == typeAuthCtrl {
			select {
			case fc.ctrlCh <- payload:
			default:
				// Ctrl queue full — auth will time out
			}
		}
		// typeReauthReq and unknown bytes are reserved / ignored.
	}
}

// OpenGate signals auth is complete; queued DATA frames begin flowing to Read().
func (fc *framedConn) OpenGate() {
	fc.openOnce.Do(func() { close(fc.authDone) })
}

func (fc *framedConn) close() {
	fc.closeOnce.Do(func() {
		close(fc.closed)
		fc.OpenGate() // unblock any blocked Read()
	})
}

// Close closes the underlying connection and unblocks pending Read/Write.
func (fc *framedConn) Close() error {
	fc.close()
	return fc.raw.Close()
}

// ---------------------------------------------------------------------------
// AuthIO — used during the auth handshake over the ctrl channel.
// ---------------------------------------------------------------------------

// WriteMsg sends a control message with the AUTH_CTRL type prefix.
func (fc *framedConn) WriteMsg(text string) error {
	payload := []byte(text)
	msg := make([]byte, 1+len(payload))
	msg[0] = typeAuthCtrl
	copy(msg[1:], payload)
	_, err := fc.raw.Write(msg)
	return err
}

// ReadMsg reads the next control message. Blocks until available or timeout/close.
func (fc *framedConn) ReadMsg() (string, error) {
	select {
	case payload, ok := <-fc.ctrlCh:
		if !ok {
			return "", io.EOF
		}
		return string(payload), nil
	case <-time.After(authTimeout):
		return "", errors.New("auth: timeout")
	case <-fc.closed:
		return "", io.EOF
	}
}

// ---------------------------------------------------------------------------
// net.Conn implementation
// ---------------------------------------------------------------------------

// Read blocks until the gate is open, then returns the next DATA frame payload.
func (fc *framedConn) Read(b []byte) (int, error) {
	select {
	case <-fc.authDone:
	case <-fc.closed:
		return 0, io.EOF
	}

	select {
	case msg, ok := <-fc.dataCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(b, msg)
		return n, nil
	case <-fc.closed:
		return 0, io.EOF
	}
}

// Write sends the payload as one or more DATA frames, chunked to maxDataPayload.
func (fc *framedConn) Write(b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		chunk := b
		if len(chunk) > maxDataPayload {
			chunk = b[:maxDataPayload]
		}
		msg := make([]byte, 1+len(chunk))
		msg[0] = typeData
		copy(msg[1:], chunk)
		if _, err := fc.raw.Write(msg); err != nil {
			return total, err
		}
		total += len(chunk)
		b = b[len(chunk):]
	}
	return total, nil
}

func (fc *framedConn) LocalAddr() net.Addr                { return fc.raw.LocalAddr() }
func (fc *framedConn) RemoteAddr() net.Addr               { return fc.raw.RemoteAddr() }
func (fc *framedConn) SetDeadline(t time.Time) error      { return nil }
func (fc *framedConn) SetReadDeadline(t time.Time) error  { return nil }
func (fc *framedConn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*framedConn)(nil)
var _ AuthIO = (*framedConn)(nil)
