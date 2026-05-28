package reticulum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// chanAuthIO is already defined in auth_test.go; these tests are in the same
// package so they share the helper.

func TestDestHeaderRoundtrip(t *testing.T) {
	addrs := []string{
		"127.0.0.1:8080",
		"example.com:443",
		"[::1]:9000",
	}
	for _, addr := range addrs {
		writerIO, readerIO := newChanAuthPair()
		require.NoError(t, writeDestHeader(writerIO, addr))
		got, err := readDestHeader(readerIO)
		require.NoError(t, err)
		require.Equal(t, addr, got)
	}
}

func TestDestHeaderEmptyReturnsError(t *testing.T) {
	_, readerIO := newChanAuthPair()
	// Inject an empty string as if the peer sent one.
	readerIO.recv <- ""
	_, err := readDestHeader(readerIO)
	require.Error(t, err)
}

func TestWriteDestHeaderEmptyReturnsError(t *testing.T) {
	writerIO, _ := newChanAuthPair()
	err := writeDestHeader(writerIO, "")
	require.Error(t, err)
}
