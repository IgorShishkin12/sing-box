package reticulum

import (
	"crypto/hmac"
	"crypto/sha256"
	"sync"
)

// Token derives a peer-specific trust token: HMAC-SHA256(password, peerHash).
// Used to recognise previously-authenticated peers without a full challenge exchange.
func Token(password, peerHash string) []byte {
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(peerHash))
	return mac.Sum(nil)
}

// TrustStore caches trust tokens for known peers in memory.
// Thread-safe. Disk persistence is not implemented; tokens are lost on restart.
type TrustStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewTrustStore returns an empty TrustStore.
func NewTrustStore() *TrustStore {
	return &TrustStore{m: make(map[string][]byte)}
}

// Store records a trust token for peerHash.
func (ts *TrustStore) Store(peerHash string, tok []byte) {
	cpy := make([]byte, len(tok))
	copy(cpy, tok)
	ts.mu.Lock()
	ts.m[peerHash] = cpy
	ts.mu.Unlock()
}

// Check reports whether tok matches the stored token for peerHash.
// Returns false if no token is stored for peerHash.
func (ts *TrustStore) Check(peerHash string, tok []byte) bool {
	ts.mu.RLock()
	stored, ok := ts.m[peerHash]
	ts.mu.RUnlock()
	if !ok {
		return false
	}
	return hmac.Equal(stored, tok)
}
