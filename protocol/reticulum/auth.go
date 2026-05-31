// Package reticulum implements symmetric mutual authentication for Reticulum bridge connections.
//
// Protocol (2 concurrent rounds, 1 RTT total):
//
//	Both → Both: [TypeRequestAuth=0x84][salt: 32 bytes]           Round 1: exchange challenges
//	Both → Both: [TypeResponseAuth=0x85][HMAC-SHA256: 32 bytes]   Round 2: prove knowledge
//
//	HMAC = HMAC-SHA256(key=password, data=ownID || peerSalt)
//
// ownID and peerID are Reticulum identity address hashes obtained from the bridge.
// Including them in the HMAC binds the proof to a specific identity, preventing
// cross-connection replay even if salts collide.
package reticulum

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"time"
)

const authTimeout = 10 * time.Second

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

	type readResult struct {
		typeByte byte
		data     []byte
		err      error
	}

	// Round 1: send TypeRequestAuth+salt and receive peer's concurrently.
	r1 := make(chan readResult, 1)
	go func() {
		typB, data, err := rw.ReadMsg()
		r1 <- readResult{typB, data, err}
	}()
	if err := rw.WriteMsg(TypeRequestAuth, ownSalt); err != nil {
		return fmt.Errorf("send round1: %w", err)
	}
	res := <-r1
	if res.err != nil {
		return fmt.Errorf("recv round1: %w", res.err)
	}
	if res.typeByte != TypeRequestAuth {
		return fmt.Errorf("round1: expected TypeRequestAuth (0x%02x), got 0x%02x", TypeRequestAuth, res.typeByte)
	}
	if len(res.data) != 32 {
		return fmt.Errorf("round1: bad length %d (want 32)", len(res.data))
	}
	peerSalt := res.data

	// Round 2: send TypeResponseAuth+MAC and receive peer's concurrently.
	ownMAC := macBound(password, ownID, peerSalt)
	r2 := make(chan readResult, 1)
	go func() {
		typB, data, err := rw.ReadMsg()
		r2 <- readResult{typB, data, err}
	}()
	if err := rw.WriteMsg(TypeResponseAuth, ownMAC); err != nil {
		return fmt.Errorf("send round2: %w", err)
	}
	res = <-r2
	if res.err != nil {
		return fmt.Errorf("recv round2: %w", res.err)
	}
	if res.typeByte != TypeResponseAuth {
		return fmt.Errorf("round2: expected TypeResponseAuth (0x%02x), got 0x%02x", TypeResponseAuth, res.typeByte)
	}
	if len(res.data) != 32 {
		return fmt.Errorf("round2: bad length %d (want 32)", len(res.data))
	}

	expected := macBound(password, peerID, ownSalt)
	if !hmac.Equal(res.data, expected) {
		return fmt.Errorf("authentication failed")
	}
	return nil
}
