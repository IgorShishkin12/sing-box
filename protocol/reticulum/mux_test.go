package reticulum

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsControl(t *testing.T) {
	for b := 0; b < 256; b++ {
		want := b >= 0x80
		require.Equal(t, want, isControl(byte(b)), "byte 0x%02x", b)
	}
}

// --- encodePacket / decodePacket ---

func TestEncodeDecodePacket_largeData(t *testing.T) {
	payload := []byte("hello")
	p := muxPacket{typeByte: TypeLargeData, connID: 0x1234, payload: payload}
	encoded := encodePacket(p)
	require.Len(t, encoded, muxHeaderSize+len(payload))
	require.Equal(t, TypeLargeData, encoded[0])
	require.Equal(t, byte(0x12), encoded[1])
	require.Equal(t, byte(0x34), encoded[2])
	require.Equal(t, payload, encoded[3:])

	decoded, err := decodePacket(encoded)
	require.NoError(t, err)
	require.Equal(t, TypeLargeData, decoded.typeByte)
	require.Equal(t, uint16(0x1234), decoded.connID)
	require.Equal(t, payload, decoded.payload)
}

func TestEncodeDecodePacket_control(t *testing.T) {
	payload := []byte("127.0.0.1:8080")
	p := muxPacket{
		typeByte: TypeNewConn,
		connID:   0x0042,
		payload:  payload,
	}
	encoded := encodePacket(p)
	require.Len(t, encoded, muxHeaderSize+len(payload))
	require.Equal(t, TypeNewConn, encoded[0])
	require.Equal(t, byte(0x00), encoded[1])
	require.Equal(t, byte(0x42), encoded[2])
	require.Equal(t, payload, encoded[3:])

	decoded, err := decodePacket(encoded)
	require.NoError(t, err)
	require.Equal(t, TypeNewConn, decoded.typeByte)
	require.Equal(t, uint16(0x0042), decoded.connID)
	require.Equal(t, payload, decoded.payload)
}

func TestDecodePacket_tooShort(t *testing.T) {
	_, err := decodePacket([]byte{0x00, 0x00}) // only 2 bytes; need ≥ 3
	require.Error(t, err)
}

// --- muxSession integration ---

// newTestMuxPair creates a client+server muxSession pair connected via net.Pipe.
// Uses byte-stream semantics; suitable for messages up to ~25 KB (64 fragments).
func newTestMuxPair(t *testing.T) (client, server *muxSession) {
	t.Helper()
	cc, sc := net.Pipe()
	t.Cleanup(func() {
		cc.Close()
		sc.Close()
	})
	client = newMuxSessionClient(cc, nil)
	server = newMuxSessionServer(sc, nil)
	return
}

// newTestMuxPairMsg creates a client+server muxSession pair backed by the
// existing chanConn (message-boundary). Required for TypeLargeData tests
// (payloads > 64*maxFragPayload) where net.Pipe byte-stream semantics break.
func newTestMuxPairMsg(t *testing.T) (client, server *muxSession) {
	t.Helper()
	cc, sc := newChanConnPair()
	t.Cleanup(func() {
		cc.Close()
		sc.Close()
	})
	client = newMuxSessionClient(cc, nil)
	server = newMuxSessionServer(sc, nil)
	return
}

func TestMuxSession_newConn(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	select {
	case sc := <-server.incomingCh:
		require.Equal(t, "127.0.0.1:8080", sc.dest)
		sc.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for incoming conn")
	}
}

func TestMuxSession_smallData(t *testing.T) {
	client, server := newTestMuxPairMsg(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	sent := []byte("hello world")
	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}

func TestMuxSession_largeData(t *testing.T) {
	client, server := newTestMuxPairMsg(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	sent := make([]byte, 1000)
	for i := range sent {
		sent[i] = byte(i % 251)
	}
	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}

func TestMuxSession_multiplexing(t *testing.T) {
	client, server := newTestMuxPairMsg(t)

	mc1, err := client.OpenConn("host1:80")
	require.NoError(t, err)
	defer mc1.Close()

	mc2, err := client.OpenConn("host2:80")
	require.NoError(t, err)
	defer mc2.Close()

	// TypeNewConn packets arrive in order, so sc1=conn1, sc2=conn2.
	sc1 := <-server.incomingCh
	require.Equal(t, "host1:80", sc1.dest)
	defer sc1.Close()

	sc2 := <-server.incomingCh
	require.Equal(t, "host2:80", sc2.dest)
	defer sc2.Close()

	msg1 := []byte("data for conn1")
	msg2 := []byte("data for conn2")

	_, err = mc1.Write(msg1)
	require.NoError(t, err)
	_, err = mc2.Write(msg2)
	require.NoError(t, err)

	buf1 := make([]byte, len(msg1))
	_, err = io.ReadFull(sc1, buf1)
	require.NoError(t, err)
	require.Equal(t, msg1, buf1)

	buf2 := make([]byte, len(msg2))
	_, err = io.ReadFull(sc2, buf2)
	require.NoError(t, err)
	require.Equal(t, msg2, buf2)
}

func TestMuxSession_connClose(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)

	sc := <-server.incomingCh

	mc.Close()

	// After TypeCloseConn is sent and processed, server Read returns EOF.
	buf := make([]byte, 4)
	_, err = sc.Read(buf)
	require.ErrorIs(t, err, io.EOF)
}

// TestMuxSession_veryLargeData verifies the TypeLargeData path for payloads that
// exceed the 64-fragment limit (> 64*maxFragPayload bytes).
// Uses chanConn (message-boundary) so the full TypeLargeData message arrives whole.
func TestMuxSession_veryLargeData(t *testing.T) {
	client, server := newTestMuxPairMsg(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	// 100 KB — well above the 64-fragment cap of ~25 KB.
	sent := make([]byte, 100*1024)
	for i := range sent {
		sent[i] = byte(i % 251)
	}

	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}


func TestMuxSession_idExhaustion(t *testing.T) {
	cc, sc := net.Pipe()
	defer cc.Close()
	defer sc.Close()

	client := newMuxSessionClient(cc, nil)
	server := newMuxSessionServer(sc, nil)

	// Drain server incoming conns so readLoop doesn't block.
	go func() {
		for mc := range server.incomingCh {
			mc.Close()
		}
	}()

	// Set counter so the next AddUint32 yields 65536, exceeding uint16 max.
	atomic.StoreUint32(&client.nextConnID, 65535)

	_, err := client.OpenConn("127.0.0.1:8080")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exhausted")
}
