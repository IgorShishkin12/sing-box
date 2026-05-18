package reticulum

import (
	"bytes"
	"encoding/binary"
	"io"
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
	// Write a header claiming 10 bytes but only provide 3.
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
