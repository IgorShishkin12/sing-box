package reticulum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrustStore_StoreAndCheck(t *testing.T) {
	ts := NewTrustStore()
	tok := Token("password", "abc123")
	ts.Store("abc123", tok)
	require.True(t, ts.Check("abc123", tok))
}

func TestTrustStore_WrongToken(t *testing.T) {
	ts := NewTrustStore()
	tok := Token("password", "abc123")
	ts.Store("abc123", tok)
	wrong := Token("wrong-password", "abc123")
	require.False(t, ts.Check("abc123", wrong))
}

func TestTrustStore_UnknownPeer(t *testing.T) {
	ts := NewTrustStore()
	require.False(t, ts.Check("nobody", []byte("anything")))
}

func TestToken_Deterministic(t *testing.T) {
	a := Token("pw", "peer1")
	b := Token("pw", "peer1")
	require.Equal(t, a, b)
}

func TestToken_DifferentPeers(t *testing.T) {
	a := Token("pw", "peer1")
	b := Token("pw", "peer2")
	require.NotEqual(t, a, b)
}
