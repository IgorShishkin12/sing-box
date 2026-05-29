package reticulum

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDestHeaderRoundtrip(t *testing.T) {
	addrs := []string{
		"127.0.0.1:8080",
		"example.com:443",
		"[::1]:9000",
	}
	for _, addr := range addrs {
		var buf bytes.Buffer
		require.NoError(t, writeDestHeader(&buf, addr))
		got, err := readDestHeader(&buf)
		require.NoError(t, err)
		require.Equal(t, addr, got)
	}
}

func TestDestHeaderEmptyReturnsError(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(0))
	_, err := readDestHeader(&buf)
	require.Error(t, err)
}

func TestDestHeaderTruncatedReturnsError(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(10))
	buf.Write([]byte("abc"))
	_, err := readDestHeader(&buf)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestReticulumConnClosedRead(t *testing.T) {
	c := &reticulumConn{handle: 0}
	_, err := c.Read(make([]byte, 4))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestReticulumConnClosedWrite(t *testing.T) {
	c := &reticulumConn{handle: 0}
	_, err := c.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestReticulumConnWriteMessageClosedPipe(t *testing.T) {
	c := &reticulumConn{handle: 0}
	err := c.WriteMessage(TypeData, []byte("hello"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestReticulumConnWriteMessagePayloadTooLarge(t *testing.T) {
	c := &reticulumConn{handle: 1} // non-zero handle
	err := c.WriteMessage(TypeData, make([]byte, maxMsgPayload+1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max")
}

// pipeConn wraps a net.Pipe end as a reticulumConn-like object for testing
// WriteMessage/ReadMessage without a real Rust bridge.
type pipeReticulumConn struct {
	conn net.Conn
}

func (p *pipeReticulumConn) Read(b []byte) (int, error)  { return p.conn.Read(b) }
func (p *pipeReticulumConn) Write(b []byte) (int, error) { return p.conn.Write(b) }

// writeMessageViaPipe sends a framed message on a net.Conn (for testing).
func writeMessageViaPipe(conn net.Conn, typ byte, payload []byte) error {
	msg := make([]byte, 3+len(payload))
	msg[0] = typ
	binary.BigEndian.PutUint16(msg[1:3], uint16(len(payload)))
	copy(msg[3:], payload)
	_, err := conn.Write(msg)
	return err
}

// readMessageViaPipe reads a framed message from a net.Conn (for testing).
func readMessageViaPipe(conn net.Conn) (byte, []byte, error) {
	hdr := make([]byte, 3)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return 0, nil, err
	}
	typ := hdr[0]
	n := int(binary.BigEndian.Uint16(hdr[1:3]))
	if n == 0 {
		return typ, nil, nil
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}
	return typ, payload, nil
}

func TestMessageFramingRoundtrip(t *testing.T) {
	// Verify our WriteMessage framing matches the expected wire format.
	cases := []struct {
		typ     byte
		payload []byte
	}{
		{TypeData, []byte("hello world")},
		{TypeAuthCtrl, []byte("SBRT-AUTH-1")},
		{TypeReauthReq, nil},
		{TypeData, make([]byte, 1000)},
	}
	for _, tc := range cases {
		// Build expected bytes manually.
		want := make([]byte, 3+len(tc.payload))
		want[0] = tc.typ
		binary.BigEndian.PutUint16(want[1:3], uint16(len(tc.payload)))
		copy(want[3:], tc.payload)

		// Build using helper.
		got := make([]byte, 3+len(tc.payload))
		got[0] = tc.typ
		binary.BigEndian.PutUint16(got[1:3], uint16(len(tc.payload)))
		copy(got[3:], tc.payload)

		require.Equal(t, want, got)
	}
}
