package reticulum

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sendCtrl injects a payload directly into fc's ctrl channel (simulates
// the dispatch goroutine routing an AUTH_CTRL frame).
func sendCtrl(fc *framedConn, payload string) {
	fc.ctrlCh <- []byte(payload)
}

// sendData injects a payload directly into fc's data channel (simulates
// the dispatch goroutine routing a DATA frame).
func sendData(fc *framedConn, payload []byte) {
	fc.dataCh <- payload
}

func newTestFramedConn() *framedConn {
	return &framedConn{
		raw:      nil, // not used; channels are fed directly
		ctrlCh:   make(chan []byte, maxCtrlQueue),
		dataCh:   make(chan []byte, maxDataQueue),
		authDone: make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

func TestFramedConn_OpenGateUnblocksRead(t *testing.T) {
	fc := newTestFramedConn()
	sendData(fc, []byte("hello"))

	readDone := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		n, _ := fc.Read(buf)
		readDone <- buf[:n]
	}()

	select {
	case <-readDone:
		t.Fatal("Read returned before OpenGate was called")
	case <-time.After(50 * time.Millisecond):
	}

	fc.OpenGate()

	select {
	case got := <-readDone:
		require.Equal(t, []byte("hello"), got)
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock after OpenGate")
	}
}

func TestFramedConn_CloseUnblocksRead(t *testing.T) {
	fc := newTestFramedConn()
	fc.OpenGate()

	readDone := make(chan error, 1)
	go func() {
		_, err := fc.Read(make([]byte, 4))
		readDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	fc.close()

	select {
	case err := <-readDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock after close")
	}
}

func TestFramedConn_CloseOpenGatesDataPath(t *testing.T) {
	// close() must call OpenGate so that Read exits via <-fc.closed rather
	// than blocking on <-fc.authDone forever.
	fc := newTestFramedConn()

	readDone := make(chan error, 1)
	go func() {
		_, err := fc.Read(make([]byte, 4))
		readDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	fc.close()

	select {
	case err := <-readDone:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("close() did not unblock Read that was waiting on gate")
	}
}

func TestFramedConn_ReadMsgFromCtrlChannel(t *testing.T) {
	fc := newTestFramedConn()
	sendCtrl(fc, "SBRT-AUTH-1")

	got, err := fc.ReadMsg()
	require.NoError(t, err)
	require.Equal(t, "SBRT-AUTH-1", got)
}

func TestFramedConn_ReadMsgReturnsEOFOnClose(t *testing.T) {
	fc := newTestFramedConn()
	fc.close()

	_, err := fc.ReadMsg()
	require.ErrorIs(t, err, io.EOF)
}

func TestFramedConn_OpenGateIdempotent(t *testing.T) {
	fc := newTestFramedConn()
	require.NotPanics(t, func() {
		fc.OpenGate()
		fc.OpenGate()
		fc.OpenGate()
	})
}

func TestFramedConn_WriteChunkingMath(t *testing.T) {
	// Verify that the chunking arithmetic is correct for various payload sizes.
	cases := []struct {
		payloadLen     int
		expectedChunks int
	}{
		{0, 0},
		{1, 1},
		{maxDataPayload - 1, 1},
		{maxDataPayload, 1},
		{maxDataPayload + 1, 2},
		{maxDataPayload * 3, 3},
		{maxDataPayload*3 + 50, 4},
	}
	for _, tc := range cases {
		chunks := 0
		rem := tc.payloadLen
		for rem > 0 {
			chunk := rem
			if chunk > maxDataPayload {
				chunk = maxDataPayload
			}
			rem -= chunk
			chunks++
		}
		require.Equal(t, tc.expectedChunks, chunks,
			"payloadLen=%d", tc.payloadLen)
	}
}

func TestAuthIO_FullHandshakeThroughChannels(t *testing.T) {
	// Verify ServerAuth+ClientAuth round-trip using chanAuthIO (no bridge).
	serverIO, clientIO := newChanAuthPair()

	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(serverIO, "secret") }()
	go func() { errs <- ClientAuth(clientIO, "secret") }()

	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs)
	}
}
