package reticulum

// trust_store.go — persistent peer authentication cache.
//
// After a successful SBRT-AUTH-1 handshake the server stores
//
//	token = hex(HMAC-SHA256(password, peerHash))
//
// to disk. On the next connection from the same peer the server checks the
// stored token without running the full handshake. If the password changes
// all stored tokens automatically become invalid (different HMAC key).
//
// The store is loaded once at Start() and written atomically on every Store()
// or Invalidate() call (temp file + rename).

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// TrustStore is a concurrent-safe, disk-backed mapping from peer hash to a
// trust token derived from the shared password.
type TrustStore struct {
	path string
	mu   sync.RWMutex
	m    map[string]trustEntry
}

type trustEntry struct {
	Token string    `json:"token"` // hex(HMAC-SHA256(password, peerHash))
	At    time.Time `json:"at"`
}

// NewTrustStore creates a TrustStore backed by the given file path.
// path may be empty; in that case the store is in-memory only (no persistence).
// The on-disk state (if any) is loaded immediately; errors are silently ignored.
func NewTrustStore(path string) *TrustStore {
	ts := &TrustStore{
		path: path,
		m:    make(map[string]trustEntry),
	}
	if path != "" {
		_ = ts.load()
	}
	return ts
}

// IsTrusted reports whether the peer identified by peerHash was previously
// authenticated with the given password.
func (ts *TrustStore) IsTrusted(peerHash, password string) bool {
	if peerHash == "" || password == "" {
		return false
	}
	ts.mu.RLock()
	e, ok := ts.m[peerHash]
	ts.mu.RUnlock()
	if !ok {
		return false
	}
	return e.Token == ts.computeToken(password, peerHash)
}

// Store records a successful authentication for peerHash and persists to disk.
func (ts *TrustStore) Store(peerHash, password string) error {
	if peerHash == "" || password == "" {
		return nil
	}
	token := ts.computeToken(password, peerHash)
	ts.mu.Lock()
	ts.m[peerHash] = trustEntry{Token: token, At: time.Now()}
	ts.mu.Unlock()
	return ts.save()
}

// Invalidate removes the trust entry for peerHash and persists the change.
func (ts *TrustStore) Invalidate(peerHash string) {
	ts.mu.Lock()
	delete(ts.m, peerHash)
	ts.mu.Unlock()
	_ = ts.save()
}

// computeToken derives a verifiable token from the password and peer hash.
func (ts *TrustStore) computeToken(password, peerHash string) string {
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(peerHash))
	return hex.EncodeToString(mac.Sum(nil))
}

func (ts *TrustStore) load() error {
	data, err := os.ReadFile(ts.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return json.Unmarshal(data, &ts.m)
}

func (ts *TrustStore) save() error {
	if ts.path == "" {
		return nil
	}
	ts.mu.RLock()
	data, err := json.Marshal(ts.m)
	ts.mu.RUnlock()
	if err != nil {
		return err
	}
	tmp := ts.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, ts.path)
}
