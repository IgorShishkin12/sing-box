// Package reticulum implements a mutual password authentication handshake for
// Reticulum bridge connections.
//
// Protocol (server speaks first):
//
//	Server → Client: "SBRT-AUTH-1"
//	Server → Client: hex(salt_s, 32 bytes)
//	Client → Server: hex(salt_c, 32 bytes)
//	Client → Server: hex(HMAC-SHA256(password, salt_s))
//	Server verifies; if wrong → caller closes the connection.
//	Server → Client: "OK"
//	Server → Client: hex(HMAC-SHA256(password, salt_c))
//	Client verifies; if wrong → caller closes the connection.
//
// Each message is exchanged as one discrete protocol message via AuthIO
// (implemented by framedConn using AUTH_CTRL-typed frames).
package reticulum

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// authTimeout is the per-message read deadline during the auth handshake.
// 30 s accommodates slow container startup and Reticulum bridge initialisation.
const authTimeout = 30 * time.Second

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
// It generates a random salt, sends it to the client together with the
// protocol header, reads the client's salt and HMAC response, verifies
// the HMAC, and if correct sends back its own HMAC proof.
//
// Returns nil on success. On failure the caller should close the connection.
func ServerAuth(io AuthIO, password string) error {
	saltS, err := generateSalt()
	if err != nil {
		return fmt.Errorf("auth: generate server salt: %w", err)
	}

	if err := io.WriteMsg("SBRT-AUTH-1"); err != nil {
		return fmt.Errorf("auth: write header: %w", err)
	}
	if err := io.WriteMsg(hex.EncodeToString(saltS)); err != nil {
		return fmt.Errorf("auth: write server salt: %w", err)
	}

	saltCHex, err := io.ReadMsg()
	if err != nil {
		return fmt.Errorf("auth: read client salt: %w", err)
	}
	saltC, err := hex.DecodeString(saltCHex)
	if err != nil || len(saltC) != 32 {
		return fmt.Errorf("auth: invalid client salt")
	}

	clientHMACHex, err := io.ReadMsg()
	if err != nil {
		return fmt.Errorf("auth: read client HMAC: %w", err)
	}
	clientHMACBytes, err := hex.DecodeString(clientHMACHex)
	if err != nil {
		return fmt.Errorf("auth: invalid client HMAC hex")
	}

	expected := computeHMAC(password, saltS)
	if !hmac.Equal(clientHMACBytes, expected) {
		return fmt.Errorf("auth: wrong password")
	}

	if err := io.WriteMsg("OK"); err != nil {
		return fmt.Errorf("auth: write OK: %w", err)
	}
	serverHMAC := computeHMAC(password, saltC)
	if err := io.WriteMsg(hex.EncodeToString(serverHMAC)); err != nil {
		return fmt.Errorf("auth: write server HMAC: %w", err)
	}

	return nil
}

// ClientAuth runs the client side of the mutual password authentication.
//
// It reads the server's header and salt, sends its own salt and HMAC
// response, reads the server's OK and HMAC proof, and verifies it.
//
// Returns nil on success. On failure the caller should close the connection.
func ClientAuth(io AuthIO, password string) error {
	header, err := io.ReadMsg()
	if err != nil {
		return fmt.Errorf("auth: read header: %w", err)
	}
	if header != "SBRT-AUTH-1" {
		return fmt.Errorf("auth: unexpected protocol header: %q", header)
	}

	saltSHex, err := io.ReadMsg()
	if err != nil {
		return fmt.Errorf("auth: read server salt: %w", err)
	}
	saltS, err := hex.DecodeString(saltSHex)
	if err != nil || len(saltS) != 32 {
		return fmt.Errorf("auth: invalid server salt")
	}

	saltC, err := generateSalt()
	if err != nil {
		return fmt.Errorf("auth: generate client salt: %w", err)
	}

	clientHMAC := computeHMAC(password, saltS)
	if err := io.WriteMsg(hex.EncodeToString(saltC)); err != nil {
		return fmt.Errorf("auth: write client salt: %w", err)
	}
	if err := io.WriteMsg(hex.EncodeToString(clientHMAC)); err != nil {
		return fmt.Errorf("auth: write client HMAC: %w", err)
	}

	status, err := io.ReadMsg()
	if err != nil {
		return fmt.Errorf("auth: read server status: %w", err)
	}
	if status != "OK" {
		return fmt.Errorf("auth: rejected by server")
	}

	serverHMACHex, err := io.ReadMsg()
	if err != nil {
		return fmt.Errorf("auth: read server HMAC: %w", err)
	}
	serverHMACBytes, err := hex.DecodeString(serverHMACHex)
	if err != nil {
		return fmt.Errorf("auth: invalid server HMAC hex")
	}

	expected := computeHMAC(password, saltC)
	if !hmac.Equal(serverHMACBytes, expected) {
		return fmt.Errorf("auth: server has wrong password")
	}

	return nil
}
