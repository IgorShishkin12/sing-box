//go:build with_reticulum

package reticulum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBridgeGetHashUnknownName(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	// Look up an unregistered name — should fail
	hash, err := BridgeGetHash("nonexistent")
	require.Error(t, err)
	require.Equal(t, "", hash)
	require.ErrorIs(t, err, ErrBridgeGetHashFailed)

	BridgeShutdown()
}

func TestBridgeGetHashAfterRegister(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	// Register a name
	err = BridgeRegisterName("alice", "alice-identity-hash")
	require.NoError(t, err)

	// Look it up — should succeed
	hash, err := BridgeGetHash("alice")
	require.NoError(t, err)
	require.NotEmpty(t, hash)

	BridgeShutdown()
}

func TestBridgeGetHashDeterministic(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	// Register
	err = BridgeRegisterName("bob", "bob-hash")
	require.NoError(t, err)

	// Look up twice
	hash1, err := BridgeGetHash("bob")
	require.NoError(t, err)

	hash2, err := BridgeGetHash("bob")
	require.NoError(t, err)

	require.Equal(t, hash1, hash2, "hash should be deterministic for same name")

	BridgeShutdown()
}

func TestBridgeRegisterNameTwice(t *testing.T) {
	err := BridgeInit("{}")
	require.NoError(t, err)

	// Register twice — should not error
	err = BridgeRegisterName("charlie", "hash1")
	require.NoError(t, err)

	err = BridgeRegisterName("charlie", "hash2")
	require.NoError(t, err)

	// Look up — should still work
	hash, err := BridgeGetHash("charlie")
	require.NoError(t, err)
	require.NotEmpty(t, hash)

	BridgeShutdown()
}