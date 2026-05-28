package reticulum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewReticulumConnReusesPreRegisteredChan is a regression test for the
// race where goOnData dropped inbound messages because the dataCh was not yet
// registered when the first packet arrived (before handleConn ran).
//
// Regression: goOnAccept now pre-creates the reticulumConn (registering its
// dataCh in connDataChans) before pushing the accept event. newReticulumConn
// must reuse that channel rather than creating a fresh one.
func TestNewReticulumConnReusesPreRegisteredChan(t *testing.T) {
	const id uint64 = 0xdeadbeef

	// Simulate what goOnAccept does: pre-register a dataCh.
	preCh := make(chan []byte, 256)
	connDataChans.Store(id, preCh)
	defer connDataChans.Delete(id)

	// Simulate an early message arriving before handleConn runs.
	preCh <- []byte("early-message")

	// Now newReticulumConn should reuse preCh, not create a new channel.
	conn := newReticulumConn(id, "local", "remote")

	// The pre-sent message must be readable through the conn — proving that
	// newReticulumConn reused the same channel rather than creating a fresh one.
	msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, []byte("early-message"), msg,
		"message sent to pre-registered channel must survive newReticulumConn")
}

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
