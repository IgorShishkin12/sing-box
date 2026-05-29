package reticulum

import (
	"crypto/hmac"
	"crypto/sha256"
	"sync"
)

// TrustStore caches authenticated peers in memory.
// The stored token is HMAC-SHA256(password, peerHash), binding identity to password
// without storing the password itself.
type TrustStore struct {
	mu sync.RWMutex
	m  map[string][]byte // peerHash → token
}

// NewTrustStore returns an empty TrustStore.
func NewTrustStore() *TrustStore {
	return &TrustStore{m: make(map[string][]byte)}
}

// TrustToken computes the token for a given password and peer hash.
func TrustToken(password, peerHash string) []byte {
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(peerHash))
	return mac.Sum(nil)
}

// Check returns true if the stored token for peerHash matches tok.
func (ts *TrustStore) Check(peerHash string, tok []byte) bool {
	ts.mu.RLock()
	stored, ok := ts.m[peerHash]
	ts.mu.RUnlock()
	return ok && hmac.Equal(stored, tok)
}

// Store saves the token for peerHash.
func (ts *TrustStore) Store(peerHash string, tok []byte) {
	ts.mu.Lock()
	ts.m[peerHash] = tok
	ts.mu.Unlock()
}
