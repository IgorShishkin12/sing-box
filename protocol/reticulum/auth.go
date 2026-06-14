// Package reticulum implements symmetric mutual authentication for Reticulum bridge connections.
//
// Protocol (send-then-receive per round):
//
//	Both → Both: [TypeRequestAuth=0x84][salt: 32 bytes]           Round 1: exchange challenges
//	Both → Both: [TypeResponseAuth=0x85][HMAC-SHA256: 32 bytes]   Round 2: prove knowledge
//
//	HMAC = HMAC-SHA256(key=password, data=ownID || peerSalt)
//
// ownID and peerID are Reticulum identity address hashes obtained from the bridge.
// Including them in the HMAC binds the proof to a specific identity, preventing
// cross-connection replay even if salts collide.
//
// Each round follows send-then-receive ordering: WriteMsg completes before ReadMsg
// is called. This matters for high-latency links (e.g. LoRa) where WriteMsg can
// block for many seconds while the TX queue drains. Starting the read timer after
// the send prevents false timeouts caused by our own transmission delay.
// The peer's packet is buffered in ctrlCh while we transmit, so ReadMsg returns
// quickly once WriteMsg completes.
package reticulum

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"time"
)

// authTimeout is the maximum idle time ReadMsg waits for the peer's next auth
// message after our own send completes. 30 s accommodates LoRa channel
// congestion (CSMA backoff, competing transmissions) without being so long that
// a genuinely lost peer hangs the connection noticeably.
const authTimeout = 30 * time.Second

// RetryPolicy controls how auth failures are retried on the same connection.
type RetryPolicy string

const (
	RetryNone   RetryPolicy = "none"
	RetryLinear RetryPolicy = "linear"
	RetryExp    RetryPolicy = "exp"
)

// retryDelay returns the delay before the next auth attempt for the given policy.
// attempt is zero-based (0 = delay before the 2nd try, after the 1st failure).
// Returns (0, false) when the policy does not retry (RetryNone or unknown).
func retryDelay(policy RetryPolicy, attempt int) (time.Duration, bool) {
	switch policy {
	case RetryLinear:
		return 5 * time.Second, true
	case RetryExp:
		// 4s, 8s, 16s, 32s, … (4 << attempt seconds)
		return time.Duration(4<<attempt) * time.Second, true
	default:
		return 0, false
	}
}

func generateSalt() ([]byte, error) {
	salt := make([]byte, 32)
	_, err := rand.Read(salt)
	return salt, err
}

// macBound computes HMAC-SHA256(key=password, data=ownID||peerSalt).
// ownID is the sender's own identity hash string; peerSalt is the challenge received from the peer.
func macBound(password, ownID string, peerSalt []byte) []byte {
	h := hmac.New(sha256.New, []byte(password))
	h.Write([]byte(ownID))
	h.Write(peerSalt)
	return h.Sum(nil)
}

// authAttempt performs one 2-round auth exchange using the provided ownSalt.
// Separating salt generation from the exchange allows callers to reuse the same
// salt across retry attempts on the same connection.
//
// Each round is send-then-receive: WriteMsg blocks until the local packet is
// queued for transmission, then ReadMsg waits for the peer's reply.
// This ordering is critical for high-latency links (e.g. LoRa) where WriteMsg
// can block for many seconds — starting the read timer before WriteMsg returns
// would cause false auth timeouts during normal operation.
func authAttempt(rw AuthIO, password, ownID, peerID string, ownSalt []byte) error {
	// Round 1: send challenge, then wait for peer's challenge.
	// Both sides do this concurrently (different connections/goroutines), so each
	// side's packet is transmitted while the other side also transmits; the peer's
	// packet is buffered in ctrlCh and ReadMsg returns quickly.
	if err := rw.WriteMsg(TypeRequestAuth, ownSalt); err != nil {
		return fmt.Errorf("send round1: %w", err)
	}
	typB, data, err := rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("recv round1: %w", err)
	}
	if typB != TypeRequestAuth {
		return fmt.Errorf("round1: expected TypeRequestAuth (0x%02x), got 0x%02x", TypeRequestAuth, typB)
	}
	if len(data) != 32 {
		return fmt.Errorf("round1: bad length %d (want 32)", len(data))
	}
	peerSalt := data

	// Round 2: send proof, then wait for peer's proof.
	ownMAC := macBound(password, ownID, peerSalt)
	if err := rw.WriteMsg(TypeResponseAuth, ownMAC); err != nil {
		return fmt.Errorf("send round2: %w", err)
	}
	typB, data, err = rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("recv round2: %w", err)
	}
	if typB != TypeResponseAuth {
		return fmt.Errorf("round2: expected TypeResponseAuth (0x%02x), got 0x%02x", TypeResponseAuth, typB)
	}
	if len(data) != 32 {
		return fmt.Errorf("round2: bad length %d (want 32)", len(data))
	}

	expected := macBound(password, peerID, ownSalt)
	if !hmac.Equal(data, expected) {
		return fmt.Errorf("authentication failed")
	}
	return nil
}

// Auth performs symmetric mutual password authentication with identity binding.
//
// ownID is this side's identity hash (transport hash or service destination hash).
// peerID is the peer's identity hash (from BridgeConnPeerHash or the known destination hash).
//
// Round 1: both sides concurrently send TypeRequestAuth + 32-byte random salt.
// Round 2: both sides concurrently send TypeResponseAuth + HMAC-SHA256(password, ownID, peerSalt).
// Each side verifies the peer's HMAC by recomputing HMAC(password, peerID, ownSalt).
//
// Returns nil on success. On failure the caller should close the connection.
func Auth(rw AuthIO, password, ownID, peerID string) error {
	ownSalt, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}
	return authAttempt(rw, password, ownID, peerID, ownSalt)
}

// authWithRetry is the testable core of AuthWithRetry; sleep is injected to allow fast tests.
// The salt is generated once and reused across all retry attempts so each side's challenge
// stays stable for the lifetime of the connection — no salt-to-response tracking needed.
func authWithRetry(rw AuthIO, password, ownID, peerID string, policy RetryPolicy, sleep func(time.Duration)) error {
	ownSalt, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}
	for attempt := 0; ; attempt++ {
		err := authAttempt(rw, password, ownID, peerID, ownSalt)
		if err == nil {
			return nil
		}
		delay, ok := retryDelay(policy, attempt)
		if !ok {
			return err
		}
		sleep(delay)
	}
}

// AuthWithRetry performs auth with the configured retry policy.
// policy "none" (or empty) is identical to Auth — one attempt, fail fast.
// policy "linear" retries every 5 s; policy "exp" retries after 4 s, 8 s, 16 s, …
// Both sides must use the same policy for retries to succeed on the same connection.
func AuthWithRetry(rw AuthIO, password, ownID, peerID string, policy RetryPolicy) error {
	return authWithRetry(rw, password, ownID, peerID, policy, time.Sleep)
}
