package reticulum

import (
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
		srvIO, cliIO := newChanAuthPair()
		writeErr := make(chan error, 1)
		go func() { writeErr <- writeDestHeader(cliIO, addr) }()
		got, err := readDestHeader(srvIO)
		require.NoError(t, err)
		require.Equal(t, addr, got)
		require.NoError(t, <-writeErr)
	}
}

func TestDestHeaderEmptyReturnsError(t *testing.T) {
	_, cliIO := newChanAuthPair()
	err := writeDestHeader(cliIO, "")
	require.Error(t, err)
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

func TestReticulumConnReadMessageClosed(t *testing.T) {
	c := &reticulumConn{handle: 0}
	_, err := c.ReadMessage()
	require.ErrorIs(t, err, io.ErrClosedPipe)
}
