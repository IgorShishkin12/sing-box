// Package reticulum implements symmetric mutual authentication for Reticulum bridge connections.
//
// Protocol (2 parallel rounds, both sides symmetric):
//
//	Both → Both: salt(32 bytes)                                    Round 1: exchange challenges
//	Both → Both: HMAC-SHA256(password, peerSalt||ownID)(32 bytes)  Round 2: prove knowledge
//
// Identities (ownID, peerID) come from the Reticulum transport — not sent over the wire.
// Reticulum's link authentication already proves each peer's identity; including it in the
// HMAC binds the password proof to a specific identity without re-transmitting it.
//
// Both rounds are concurrent (1 RTT total). Messages flow via AuthIO (TypeAuthCtrl channel).
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

// macBound computes HMAC-SHA256(password, challengeSalt || senderID).
// challengeSalt is the salt received from the peer (the challenge to answer).
// senderID commits the proof to the sender's own identity.
func macBound(password string, challengeSalt, senderID []byte) []byte {
	h := hmac.New(sha256.New, []byte(password))
	h.Write(challengeSalt)
	h.Write(senderID)
	return h.Sum(nil)
}

// Auth performs symmetric mutual password authentication with identity binding.
//
// ownID and peerID come from the Reticulum transport (not transmitted over the wire).
// Round 1: exchange 32-byte random salts.
// Round 2: each side sends HMAC(password, peerSalt||ownID) and verifies HMAC(password, ownSalt||peerID).
//
// Returns nil on success. On failure the caller should close the connection.
func Auth(rw AuthIO, password, ownID, peerID string) error {
	ownSalt, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}

	type readResult struct {
		typeByte byte
		data []byte
		err  error
	}

	// Round 1: send TypeRequestAuth+salt(32) and receive peer's concurrently.
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
		return fmt.Errorf("round1 length %d (want 32)", len(res.data))
	}
	peerSalt := res.data

	// Round 2: send TypeResponseAuth+MAC and receive peer's concurrently.
	ownMAC := macBound(password, peerSalt, []byte(ownID))
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
		return fmt.Errorf("round2 length %d (want 32)", len(res.data))
	}

	expected := macBound(password, ownSalt, []byte(peerID))
	if !hmac.Equal(res.data, expected) {
		return fmt.Errorf("authentication failed")
	}
	return nil
}
