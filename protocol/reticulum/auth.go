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

// authTimeout is the overall deadline for each round within authAttempt.
// 30 s accommodates LoRa channel congestion (CSMA backoff, competing
// transmissions) without being so long that a genuinely lost peer hangs
// the connection noticeably.
const authTimeout = 30 * time.Second

// retransmitRound1Interval is the suggested sleep between authWithRetry attempts
// (used by retryDelay for RetryLinear). It is NOT the Round 1 read deadline;
// that uses authTimeout so a full LoRa round-trip (can be 8–12 s at SF8 BW62.5)
// fits within a single attempt.
const retransmitRound1Interval = 5 * time.Second

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
// Separating salt generation from the exchange allows authWithRetry to reuse the
// same salt across retries on the same connection.
//
// Round 1 sends TypeRequestAuth once and waits up to authTimeout (30 s) for
// the peer's challenge. 30 s accommodates LoRa RTTs up to ~12 s at SF8 BW62.5.
// On timeout authAttempt returns an error; the caller (authWithRetry) is responsible
// for backing off and retrying. On lossy LoRa links the peer's framedConn buffers
// arriving challenges in ctrlCh from the moment the link is accepted, so a retry
// arriving after a long identify exchange is delivered immediately when the peer's
// auth starts.
//
// Round 2 skips stale TypeRequestAuth messages that may have been buffered from the
// peer's own retransmits before they received our Round 1 response.
func authAttempt(rw AuthIO, password, ownID, peerID string, ownSalt []byte) error {
	// Round 1: send challenge and wait up to authTimeout for the peer's challenge.
	// authTimeout (30 s) must exceed LoRa RTT (up to ~12 s at SF8 BW62.5).
	if err := rw.WriteMsg(TypeRequestAuth, ownSalt); err != nil {
		return fmt.Errorf("send round1: %w", err)
	}
	// Round 1 read loop: skip TypeResponseAuth (0x85) that may arrive if the peer
	// already received our challenge and responded before its own challenge reached us.
	// This happens on lossy LoRa links where the peer's TypeRequestAuth was lost in
	// transit.  We keep waiting; the peer will retry its TypeRequestAuth after its
	// own Round 2 timeout, at which point we can complete Round 1.
	var typB byte
	var data []byte
	for {
		var err error
		typB, data, err = rw.ReadMsgDeadline(time.Now().Add(authTimeout))
		if err != nil {
			return fmt.Errorf("recv round1: %w", err)
		}
		if typB == TypeResponseAuth {
			continue // peer's Round 2 response arrived before their Round 1 — skip
		}
		break
	}
	if typB != TypeRequestAuth {
		return fmt.Errorf("round1: expected TypeRequestAuth (0x%02x), got 0x%02x", TypeRequestAuth, typB)
	}
	if len(data) != 32 {
		return fmt.Errorf("round1: bad length %d (want 32)", len(data))
	}
	peerSalt := data

	// Round 2: send proof, receive and verify peer's proof. Skip stale TypeRequestAuth
	// messages buffered from the peer's own retransmits.
	ownMAC := macBound(password, ownID, peerSalt)
	if err := rw.WriteMsg(TypeResponseAuth, ownMAC); err != nil {
		return fmt.Errorf("send round2: %w", err)
	}
	var typB2 byte
	var data2 []byte
	for {
		var err error
		typB2, data2, err = rw.ReadMsg()
		if err != nil {
			return fmt.Errorf("recv round2: %w", err)
		}
		if typB2 == TypeRequestAuth {
			continue // stale Round 1 retransmit from peer — skip
		}
		break
	}
	if typB2 != TypeResponseAuth {
		return fmt.Errorf("round2: expected TypeResponseAuth (0x%02x), got 0x%02x", TypeResponseAuth, typB2)
	}
	if len(data2) != 32 {
		return fmt.Errorf("round2: bad length %d (want 32)", len(data2))
	}
	expected := macBound(password, peerID, ownSalt)
	if !hmac.Equal(data2, expected) {
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
