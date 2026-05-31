package reticulum

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// chanConn is a net.Conn that preserves message boundaries:
// each Write becomes exactly one Read on the peer side.
type chanConn struct {
	readCh  chan []byte
	writeCh chan []byte
	closed  chan struct{}
}

func newChanConnPair() (*chanConn, *chanConn) {
	aTob := make(chan []byte, 64)
	bToa := make(chan []byte, 64)
	return &chanConn{readCh: bToa, writeCh: aTob, closed: make(chan struct{})},
		&chanConn{readCh: aTob, writeCh: bToa, closed: make(chan struct{})}
}

func (c *chanConn) Read(b []byte) (int, error) {
	select {
	case msg, ok := <-c.readCh:
		if !ok {
			return 0, io.EOF
		}
		return copy(b, msg), nil
	case <-c.closed:
		return 0, io.EOF
	}
}

func (c *chanConn) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	cpy := make([]byte, len(b))
	copy(cpy, b)
	select {
	case c.writeCh <- cpy:
		return len(b), nil
	case <-c.closed:
		return 0, io.ErrClosedPipe
	}
}

func (c *chanConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

func (c *chanConn) LocalAddr() net.Addr              { return reticulumAddr{"test", "local"} }
func (c *chanConn) RemoteAddr() net.Addr             { return reticulumAddr{"test", "remote"} }
func (c *chanConn) SetDeadline(time.Time) error      { return nil }
func (c *chanConn) SetReadDeadline(time.Time) error  { return nil }
func (c *chanConn) SetWriteDeadline(time.Time) error { return nil }

// --- framed_conn tests ---

// TestFramedConn_AuthCtrlRouting verifies TypeRequestAuth messages are delivered via ReadMsg.
func TestFramedConn_AuthCtrlRouting(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	want := []byte("hello-auth")
	go func() {
		msg := append([]byte{TypeRequestAuth}, want...)
		peerConn.Write(msg)
	}()

	typB, got, err := fc.ReadMsg()
	require.NoError(t, err)
	require.Equal(t, TypeRequestAuth, typB)
	require.Equal(t, want, got)
}

// TestFramedConn_WriteMsgPrependsTypeByte verifies WriteMsg prefixes the given type byte.
func TestFramedConn_WriteMsgPrependsTypeByte(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	payload := []byte("auth-payload")
	require.NoError(t, fc.WriteMsg(TypeRequestAuth, payload))

	buf := make([]byte, 64)
	n, err := peerConn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, TypeRequestAuth, buf[0])
	require.Equal(t, payload, buf[1:n])
}

// TestFramedConn_TypeResponseAuthRouting verifies TypeResponseAuth also routes to ctrlCh.
func TestFramedConn_TypeResponseAuthRouting(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	want := make([]byte, 32)
	for i := range want {
		want[i] = byte(i)
	}
	go func() {
		msg := append([]byte{TypeResponseAuth}, want...)
		peerConn.Write(msg)
	}()

	typB, got, err := fc.ReadMsg()
	require.NoError(t, err)
	require.Equal(t, TypeResponseAuth, typB)
	require.Equal(t, want, got)
}

// TestFramedConn_DataBlockedUntilGate verifies Read blocks before OpenGate.
func TestFramedConn_DataBlockedUntilGate(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	// Send a data packet (non-auth first byte) from the peer.
	dataMsg := []byte{0x40, 0x00, 0x01, 'h', 'i'} // e.g. a mux data packet
	peerConn.Write(dataMsg)

	// Read should block until gate opens.
	readDone := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		n, _ := fc.Read(buf)
		readDone <- buf[:n]
	}()

	// Give the goroutine time to block.
	select {
	case <-readDone:
		t.Fatal("Read returned before OpenGate")
	case <-time.After(50 * time.Millisecond):
	}

	fc.OpenGate()

	select {
	case got := <-readDone:
		require.Equal(t, dataMsg, got)
	case <-time.After(time.Second):
		t.Fatal("Read did not return after OpenGate")
	}
}

// TestFramedConn_DataAfterGate verifies Read returns data immediately once gate is open.
func TestFramedConn_DataAfterGate(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	fc.OpenGate()

	dataMsg := []byte{0x40, 0x00, 0x01, 'd', 'a', 't', 'a'}
	peerConn.Write(dataMsg)

	buf := make([]byte, 64)
	n, err := fc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, dataMsg, buf[:n])
}

// TestFramedConn_WritePassthrough verifies Write sends bytes unchanged.
func TestFramedConn_WritePassthrough(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	want := []byte{0x82, 0x00, 0x01, 's', 'r', 'v'} // TypeNewConn mux packet
	n, err := fc.Write(want)
	require.NoError(t, err)
	require.Equal(t, len(want), n)

	buf := make([]byte, 64)
	m, err := peerConn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, want, buf[:m])
}

// TestFramedConn_CloseUnblocksRead verifies Close causes a blocked Read to return.
func TestFramedConn_CloseUnblocksRead(t *testing.T) {
	_, myConn := newChanConnPair()
	fc := newFramedConn(myConn)

	fc.OpenGate()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, err := fc.Read(buf)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	fc.Close()

	select {
	case err := <-done:
		// io.EOF or io.ErrClosedPipe are both acceptable
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock after Close")
	}
}

// TestFramedConn_AuthAndDataInterleaved verifies auth messages and data messages
// are both routed correctly when sent in sequence.
func TestFramedConn_AuthAndDataInterleaved(t *testing.T) {
	peerConn, myConn := newChanConnPair()
	fc := newFramedConn(myConn)
	defer fc.Close()

	// Send auth message (TypeRequestAuth), then data message.
	peerConn.Write(append([]byte{TypeRequestAuth}, []byte("challenge")...))
	peerConn.Write([]byte{0x40, 0x00, 0x02, 'd'})

	// Auth arrives via ReadMsg.
	typB, payload, err := fc.ReadMsg()
	require.NoError(t, err)
	require.Equal(t, TypeRequestAuth, typB)
	require.Equal(t, []byte("challenge"), payload)

	// Data is buffered; won't arrive until gate opens.
	fc.OpenGate()
	buf := make([]byte, 64)
	n, err := fc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, []byte{0x40, 0x00, 0x02, 'd'}, buf[:n])
}
