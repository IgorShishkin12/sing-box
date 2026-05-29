package reticulum

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// chanAuthIO and newChanAuthIOPair are defined in auth_test.go.

func TestFramedConnAuthIO_RoundTrip(t *testing.T) {
	server, client := newChanAuthIOPair()
	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(server, "pw") }()
	go func() { errs <- ClientAuth(client, "pw") }()
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs)
	}
}

func TestTrustStore_StoreAndCheck(t *testing.T) {
	ts := NewTrustStore()
	tok := TrustToken("password", "abc123")

	require.False(t, ts.Check("abc123", tok))
	ts.Store("abc123", tok)
	require.True(t, ts.Check("abc123", tok))

	require.False(t, ts.Check("other", tok))
	require.False(t, ts.Check("abc123", []byte("wrong")))
}

func TestTrustStore_TokenDiffersByPassword(t *testing.T) {
	tok1 := TrustToken("pass1", "hash")
	tok2 := TrustToken("pass2", "hash")
	require.NotEqual(t, tok1, tok2)
}

func TestFramedConnTypeConstants(t *testing.T) {
	require.Equal(t, byte(0), TypeData&0x80, "data type must have bit-7 = 0")
	require.Equal(t, byte(0x80), TypeAuthCtrl&0x80, "auth ctrl must have bit-7 = 1")
	require.Equal(t, byte(0x80), TypeReauthReq&0x80, "reauth req must have bit-7 = 1")
	require.NotEqual(t, TypeAuthCtrl, TypeReauthReq)
}

// negotiateServerAuthIO mirrors negotiateServerAuth but accepts any AuthIO — used in unit tests
// to avoid requiring a real *framedConn.
func negotiateServerAuthIO(fc AuthIO, password, peerHash string, ts *TrustStore) error {
	if peerHash != "" {
		tok := TrustToken(password, peerHash)
		if ts.Check(peerHash, tok) {
			return fc.WriteMsg([]byte{0x01})
		}
	}
	if err := fc.WriteMsg([]byte{0x00}); err != nil {
		return err
	}
	if err := ServerAuth(fc, password); err != nil {
		return err
	}
	if peerHash != "" {
		ts.Store(peerHash, TrustToken(password, peerHash))
	}
	return nil
}

// negotiateClientAuthIO mirrors negotiateClientAuth but accepts any AuthIO.
func negotiateClientAuthIO(fc AuthIO, password, destHash string, ts *TrustStore) error {
	hint, err := fc.ReadMsg()
	if err != nil {
		return err
	}
	if len(hint) > 0 && hint[0] == 0x01 {
		return nil
	}
	if err := ClientAuth(fc, password); err != nil {
		return err
	}
	if destHash != "" {
		ts.Store(destHash, TrustToken(password, destHash))
	}
	return nil
}

func TestNegotiateAuth_FullFlow(t *testing.T) {
	ts := NewTrustStore()
	serverIO, clientIO := newChanAuthIOPair()
	errs := make(chan error, 2)

	go func() { errs <- negotiateServerAuthIO(serverIO, "password", "peer1", ts) }()
	go func() { errs <- negotiateClientAuthIO(clientIO, "password", "peer1", ts) }()
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs)
	}

	// Second round — peer is now trusted, should skip full auth.
	serverIO2, clientIO2 := newChanAuthIOPair()
	go func() { errs <- negotiateServerAuthIO(serverIO2, "password", "peer1", ts) }()
	go func() { errs <- negotiateClientAuthIO(clientIO2, "password", "peer1", ts) }()
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs)
	}
}

func TestNegotiateAuth_TrustShortcut(t *testing.T) {
	ts := NewTrustStore()
	ts.Store("dest1", TrustToken("pw", "dest1"))

	serverIO, clientIO := newChanAuthIOPair()
	errs := make(chan error, 2)
	go func() { errs <- negotiateServerAuthIO(serverIO, "pw", "dest1", ts) }()
	go func() { errs <- negotiateClientAuthIO(clientIO, "pw", "dest1", ts) }()
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs, "trusted peer should skip full auth")
	}
}

func TestChanAuthIO_EOFOnClose(t *testing.T) {
	a, _ := newChanAuthIOPair()
	close(a.recvCh)
	_, err := a.ReadMsg()
	require.ErrorIs(t, err, io.EOF)
}
