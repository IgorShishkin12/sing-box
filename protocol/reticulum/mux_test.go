package reticulum

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- encodeDataByte / decodeDataByte ---

func TestEncodeDataByte_single(t *testing.T) {
	// total=1, partIdx=0 → isLast=1, partIndex=0 → 0x40
	require.Equal(t, byte(0x40), encodeDataByte(1, 0))
}

func TestEncodeDataByte_twoOfThree(t *testing.T) {
	// total=3, partIdx=1 → isLast=0, partIndex=1 → 0x01
	require.Equal(t, byte(0x01), encodeDataByte(3, 1))
}

func TestDecodeDataByte_zero(t *testing.T) {
	// 0x00: isLast=false, partIndex=0
	isLast, idx := decodeDataByte(0x00)
	require.False(t, isLast)
	require.Equal(t, 0, idx)
}

func TestDecodeDataByte_multipart(t *testing.T) {
	// middle of a 3-part message: isLast=false, partIndex=1
	b := encodeDataByte(3, 1)
	isLast, idx := decodeDataByte(b)
	require.False(t, isLast)
	require.Equal(t, 1, idx)
}

func TestDecodeDataByte_max(t *testing.T) {
	// last of 64 parts: isLast=true, partIndex=63 → 0x7F
	b := encodeDataByte(64, 63)
	require.Equal(t, byte(0x7F), b)
	isLast, idx := decodeDataByte(b)
	require.True(t, isLast)
	require.Equal(t, 63, idx)
}

func TestIsControl(t *testing.T) {
	for b := 0; b < 256; b++ {
		want := b >= 0x80
		require.Equal(t, want, isControl(byte(b)), "byte 0x%02x", b)
	}
}

// --- encodePacket / decodePacket ---

func TestEncodeDecodePacket_data(t *testing.T) {
	payload := []byte("hello")
	p := muxPacket{
		typeByte:  encodeDataByte(1, 0), // single-frag: isLast=true, partIndex=0 → 0x40
		connID:    0x1234,
		payload:   payload,
		isLast:    true,
		partIndex: 0,
	}
	encoded := encodePacket(p)
	require.Len(t, encoded, muxHeaderSize+len(payload))
	require.Equal(t, byte(0x40), encoded[0])
	require.Equal(t, byte(0x12), encoded[1])
	require.Equal(t, byte(0x34), encoded[2])
	require.Equal(t, payload, encoded[3:])

	decoded, err := decodePacket(encoded)
	require.NoError(t, err)
	require.Equal(t, p.typeByte, decoded.typeByte)
	require.Equal(t, p.connID, decoded.connID)
	require.Equal(t, payload, decoded.payload)
	require.True(t, decoded.isLast)
	require.Equal(t, 0, decoded.partIndex)
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

// --- fragment ---

func TestFragment_small(t *testing.T) {
	data := make([]byte, 100)
	parts := fragment(data)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], 100)
}

func TestFragment_exact(t *testing.T) {
	data := make([]byte, maxFragPayload)
	parts := fragment(data)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], maxFragPayload)
}

func TestFragment_split(t *testing.T) {
	data := make([]byte, maxFragPayload+1)
	parts := fragment(data)
	require.Len(t, parts, 2)
	require.Len(t, parts[0], maxFragPayload)
	require.Len(t, parts[1], 1)
}

func TestFragment_mss(t *testing.T) {
	data := make([]byte, 6*maxFragPayload) // 1182 bytes
	parts := fragment(data)
	require.Len(t, parts, 6)
	for i, p := range parts {
		require.Len(t, p, maxFragPayload, "fragment %d wrong size", i)
	}
}

func TestFragment_empty(t *testing.T) {
	for _, input := range [][]byte{nil, {}} {
		parts := fragment(input)
		require.Len(t, parts, 1)
		require.Empty(t, parts[0])
	}
}

// --- fragBuffer ---

func TestFragBuffer_singlePart(t *testing.T) {
	fb := &fragBuffer{}
	assembled, done := fb.addPart(0, true, []byte("hello"))
	require.True(t, done)
	require.Equal(t, []byte("hello"), assembled)
}

func TestFragBuffer_multiPart(t *testing.T) {
	fb := &fragBuffer{}

	assembled, done := fb.addPart(0, false, []byte("hel"))
	require.False(t, done)
	require.Nil(t, assembled)

	assembled, done = fb.addPart(1, false, []byte("lo "))
	require.False(t, done)
	require.Nil(t, assembled)

	assembled, done = fb.addPart(2, true, []byte("world"))
	require.True(t, done)
	require.Equal(t, []byte("hello world"), assembled)
}

// --- muxSession integration ---

// newTestMuxPair creates a client+server muxSession pair connected via net.Pipe.
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
	client, server := newTestMuxPair(t)

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
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	// 1000 bytes → ceil(1000/197) = 6 fragments
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
	client, server := newTestMuxPair(t)

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
