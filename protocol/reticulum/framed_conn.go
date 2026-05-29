package reticulum

// framed_conn.go — type-prefixed message framing on top of reticulumConn.
//
// Every reticulum message is a discrete packet (BridgeWrite→goOnData preserves
// boundaries). We prefix each message with exactly one type byte:
//
//	0b0xxxxxxx  DATA      — user traffic; bits 6-0 reserved for future use
//	0b10000000  AUTH_CTRL — auth handshake control messages + dest header
//	0b10000001  REAUTH_REQ — server→client: please re-authenticate
//
// During auth the gate is closed: DATA frames arriving in that window are
// buffered (up to maxDataQueue). Once the gate opens (OpenGate), buffered
// frames are released to Read() in FIFO order. Frames beyond the cap are
// silently dropped (acceptable per protocol design).

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	typeMask     byte = 0b10000000
	typeData     byte = 0b00000000 // MSB = 0
	typeAuthCtrl byte = 0b10000000 // 0x80
	typeReauthReq byte = 0b10000001 // 0x81

	maxDataQueue = 256 // pre-gate data frames before dropping
	maxCtrlQueue = 32  // ctrl frame buffer (auth messages)

	// maxFramePayload is the maximum user-data bytes per Reticulum DATA packet.
	// Reticulum's PACKET_MDU is 464 bytes; after link-layer encryption overhead
	// (~32 bytes) and our 1-byte type prefix the safe plaintext budget is ~431 B.
	// We use 400 to leave margin for encryption variants.
	maxFramePayload = 400
)

// AuthIO is the transport interface used by ServerAuth / ClientAuth.
// Each WriteMsg / ReadMsg corresponds to one discrete protocol message.
type AuthIO interface {
	WriteMsg(text string) error
	ReadMsg() (string, error)
}

// framedConn wraps a *reticulumConn and implements net.Conn + AuthIO.
type framedConn struct {
	raw     *reticulumConn
	ctrlCh  chan []byte     // inbound AUTH_CTRL payloads
	dataCh  chan []byte     // inbound DATA payloads (queued until gate opens)
	authDone chan struct{}  // closed by OpenGate()
	openOnce sync.Once
	closed  chan struct{}
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

// dispatch reads raw messages and routes them by type. Must run in its own goroutine.
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

		if typeByte&typeMask == typeData { // MSB = 0 → DATA
			select {
			case fc.dataCh <- payload:
			default:
				// Queue full — drop frame (allowed packet loss during auth)
				pkgTrace("[framed_conn] dispatch: dataCh full, dropped ", len(payload), "B DATA frame")
			}
		} else if typeByte == typeAuthCtrl {
			select {
			case fc.ctrlCh <- payload:
			default:
				// Ctrl queue full — auth will time out
				pkgTrace("[framed_conn] dispatch: ctrlCh full, dropped AUTH_CTRL frame")
			}
		} else if typeByte == typeReauthReq {
			// Re-auth requested by peer. TODO: trigger re-auth goroutine.
			// For now log and ignore; the connection continues.
		}
		// All other type bytes are reserved and ignored.
	}
}

// OpenGate signals that auth is complete; queued DATA frames begin flowing to Read().
func (fc *framedConn) OpenGate() {
	fc.openOnce.Do(func() { close(fc.authDone) })
}

func (fc *framedConn) close() {
	fc.closeOnce.Do(func() {
		close(fc.closed)
		// Also open the gate so any blocked Read() can exit with EOF.
		fc.OpenGate()
	})
}

// Close closes the underlying connection and unblocks any pending Read/Write.
func (fc *framedConn) Close() error {
	fc.close()
	return fc.raw.Close()
}

// ---------------------------------------------------------------------------
// AuthIO — used exclusively during the auth handshake (no gate required).
// ---------------------------------------------------------------------------

// WriteMsg sends a control message (AUTH_CTRL type byte prefix).
func (fc *framedConn) WriteMsg(text string) error {
	payload := []byte(text)
	msg := make([]byte, 1+len(payload))
	msg[0] = typeAuthCtrl
	copy(msg[1:], payload)
	_, err := fc.raw.Write(msg)
	return err
}

// ReadMsg reads the next control message. Returns error on timeout or close.
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
	// Wait for auth to complete (gate open) or connection closed.
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
		// TODO: handle n < len(msg) (oversized frame — callers use large buffers).
		return n, nil
	case <-fc.closed:
		return 0, io.EOF
	}
}

// Write sends one or more DATA frames. Payloads larger than maxFramePayload
// are split into multiple frames so each fits within the Reticulum packet MDU.
func (fc *framedConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	total := 0
	for len(b) > 0 {
		chunk := b
		if len(chunk) > maxFramePayload {
			chunk = b[:maxFramePayload]
		}
		msg := make([]byte, 1+len(chunk))
		msg[0] = typeData
		copy(msg[1:], chunk)
		n, err := fc.raw.Write(msg)
		if err != nil {
			return total, err
		}
		if n > 0 {
			n-- // subtract the type byte
		}
		total += n
		b = b[len(chunk):]
	}
	return total, nil
}

func (fc *framedConn) LocalAddr() net.Addr                { return fc.raw.LocalAddr() }
func (fc *framedConn) RemoteAddr() net.Addr               { return fc.raw.RemoteAddr() }
func (fc *framedConn) SetDeadline(t time.Time) error      { return nil }
func (fc *framedConn) SetReadDeadline(t time.Time) error  { return nil }
func (fc *framedConn) SetWriteDeadline(t time.Time) error { return nil }

// Compile-time interface checks.
var _ net.Conn = (*framedConn)(nil)
var _ AuthIO = (*framedConn)(nil)
