package reticulum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewReticulumConnReusesPreregisteredChannel is a regression test for the
// race where goOnData fires before DialContext calls newReticulumConn.
//
// Before the fix, newReticulumConn always created a new channel and
// overwrote connDataChans, so packets delivered via goOnData in the
// window between spawn_link_data_reader and newReticulumConn were silently
// dropped — causing the first auth message (server trust hint) to be lost
// and shifting the auth framing, which produced the observed error:
//   "auth: unexpected protocol header: <server-salt-hex>"
func TestNewReticulumConnReusesPreregisteredChannel(t *testing.T) {
	const fakeID uint64 = 0xdeadbeef

	// Simulate goOnConnect/goOnAccept pre-registering the channel.
	preCh := make(chan []byte, 256)
	connDataChans.Store(fakeID, preCh)

	// Simulate goOnData delivering a packet during the race window, before
	// newReticulumConn has been called.
	earlyPacket := []byte{0x80, 0x00} // trust-hint frame
	preCh <- earlyPacket

	// Now simulate DialContext calling newReticulumConn.
	conn := newReticulumConn(fakeID, "local", "remote")
	t.Cleanup(func() { connDataChans.Delete(fakeID) })

	// The packet buffered during the race window must be readable.
	msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, earlyPacket, msg)
}

// TestNewReticulumConnCreatesChannelWhenNonePreregistered verifies the
// fallback path (no pre-registered channel) still works.
func TestNewReticulumConnCreatesChannelWhenNonePreregistered(t *testing.T) {
	const fakeID uint64 = 0xcafebabe

	conn := newReticulumConn(fakeID, "local", "remote")
	t.Cleanup(func() { connDataChans.Delete(fakeID) })

	require.NotNil(t, conn.dataCh)
	_, ok := connDataChans.Load(fakeID)
	require.True(t, ok, "connDataChans must be populated by newReticulumConn")
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
