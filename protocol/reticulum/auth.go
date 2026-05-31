// Package reticulum implements a mutual password authentication handshake for
// Reticulum bridge connections.
//
// Protocol (server speaks first):
//
//	Server → Client: [0x01][salt_s 32 bytes]                 (version + server salt)
//	Client → Server: [salt_c 32 bytes][HMAC-SHA256(password, salt_s) 32 bytes]
//	Server verifies; on failure returns error (caller closes the connection).
//	Server → Client: [0x00][HMAC-SHA256(password, salt_c) 32 bytes]  (OK + proof)
//	Client verifies; on failure returns error.
//
// All messages are exchanged via AuthIO (typed control messages, type byte = TypeAuthCtrl).
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

func computeHMAC(password string, salt []byte) []byte {
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write(salt)
	return mac.Sum(nil)
}

// ServerAuth runs the server side of the mutual password authentication.
//
// Sends a version byte + random salt, reads the client's salt and HMAC,
// verifies the HMAC, and if correct sends back OK + server HMAC proof.
//
// Returns nil on success. On failure the caller should close the connection.
func ServerAuth(rw AuthIO, password string) error {
	saltS, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate server salt: %w", err)
	}

	// Msg 1: version=0x01 + server salt (33 bytes).
	msg1 := make([]byte, 33)
	msg1[0] = 0x01
	copy(msg1[1:], saltS)
	if err := rw.WriteMsg(msg1); err != nil {
		return fmt.Errorf("write challenge: %w", err)
	}

	// Msg 2: client salt (32) + HMAC(password, saltS) (32) = 64 bytes.
	msg2, err := rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if len(msg2) != 64 {
		return fmt.Errorf("invalid response length: %d (want 64)", len(msg2))
	}
	saltC := msg2[:32]
	clientHMAC := msg2[32:]

	expected := computeHMAC(password, saltS)
	if !hmac.Equal(clientHMAC, expected) {
		// Send explicit rejection so the client's ReadMsg unblocks immediately.
		_ = rw.WriteMsg([]byte{0x01})
		return fmt.Errorf("wrong password")
	}

	// Msg 3: 0x00=OK + HMAC(password, saltC) (33 bytes).
	msg3 := make([]byte, 33)
	msg3[0] = 0x00
	copy(msg3[1:], computeHMAC(password, saltC))
	if err := rw.WriteMsg(msg3); err != nil {
		return fmt.Errorf("write confirmation: %w", err)
	}
	return nil
}

// ClientAuth runs the client side of the mutual password authentication.
//
// Reads the server's challenge, sends client salt + HMAC response,
// reads the server's OK and verifies the server's HMAC proof.
//
// Returns nil on success. On failure the caller should close the connection.
func ClientAuth(rw AuthIO, password string) error {
	// Msg 1: version byte + server salt.
	msg1, err := rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("read challenge: %w", err)
	}
	if len(msg1) != 33 || msg1[0] != 0x01 {
		return fmt.Errorf("unexpected challenge (len=%d, ver=0x%02x)", len(msg1), msg1[0])
	}
	saltS := msg1[1:]

	saltC, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate client salt: %w", err)
	}

	// Msg 2: client salt (32) + HMAC(password, saltS) (32).
	msg2 := make([]byte, 64)
	copy(msg2[:32], saltC)
	copy(msg2[32:], computeHMAC(password, saltS))
	if err := rw.WriteMsg(msg2); err != nil {
		return fmt.Errorf("write response: %w", err)
	}

	// Msg 3: OK byte + HMAC(password, saltC).
	msg3, err := rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("read confirmation: %w", err)
	}
	if msg3[0] != 0x00 {
		return fmt.Errorf("rejected by server")
	}
	if len(msg3) != 33 {
		return fmt.Errorf("invalid confirmation length: %d (want 33)", len(msg3))
	}

	serverHMAC := msg3[1:]
	expected := computeHMAC(password, saltC)
	if !hmac.Equal(serverHMAC, expected) {
		return fmt.Errorf("server has wrong password")
	}
	return nil
}
