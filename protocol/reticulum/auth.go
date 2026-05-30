// Package reticulum implements a mutual password authentication handshake for
// Reticulum bridge connections.
//
// Protocol (server speaks first):
//
//	Server → Client: "SBRT-AUTH-1" (AuthIO message)
//	Server → Client: hex(salt_s, 32 bytes)
//	Client → Server: hex(salt_c, 32 bytes)
//	Client → Server: hex(HMAC-SHA256(password, salt_s))
//	Server verifies; if wrong → returns error (caller should close the connection).
//	Server → Client: "OK"
//	Server → Client: hex(HMAC-SHA256(password, salt_c))
//	Client verifies; if wrong → returns error.
package reticulum

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

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

// readMsgWithTimeout reads one message via rw with a deadline.
func readMsgWithTimeout(rw AuthIO, timeout time.Duration) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		data, err := rw.ReadMsg()
		ch <- result{data, err}
	}()
	select {
	case r := <-ch:
		return r.data, r.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("auth timeout")
	}
}

// ServerAuth runs the server side of the mutual password authentication.
//
// Returns nil on success. On failure the caller should close the connection.
func ServerAuth(rw AuthIO, password string) error {
	saltS, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate server salt: %w", err)
	}

	if err := rw.WriteMsg([]byte("SBRT-AUTH-1")); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if err := rw.WriteMsg([]byte(hex.EncodeToString(saltS))); err != nil {
		return fmt.Errorf("write server salt: %w", err)
	}

	saltCHexB, err := readMsgWithTimeout(rw, authTimeout)
	if err != nil {
		return fmt.Errorf("read client salt: %w", err)
	}
	saltC, err := hex.DecodeString(string(saltCHexB))
	if err != nil || len(saltC) != 32 {
		return fmt.Errorf("invalid client salt")
	}

	clientHMACHexB, err := readMsgWithTimeout(rw, authTimeout)
	if err != nil {
		return fmt.Errorf("read client HMAC: %w", err)
	}
	clientHMACBytes, err := hex.DecodeString(string(clientHMACHexB))
	if err != nil {
		return fmt.Errorf("invalid client HMAC hex")
	}

	expected := computeHMAC(password, saltS)
	if !hmac.Equal(clientHMACBytes, expected) {
		_ = rw.WriteMsg([]byte("FAIL")) // notify client so it doesn't wait for timeout
		return fmt.Errorf("wrong password")
	}

	if err := rw.WriteMsg([]byte("OK")); err != nil {
		return fmt.Errorf("write OK: %w", err)
	}
	if err := rw.WriteMsg([]byte(hex.EncodeToString(computeHMAC(password, saltC)))); err != nil {
		return fmt.Errorf("write server HMAC: %w", err)
	}

	return nil
}

// ClientAuth runs the client side of the mutual password authentication.
//
// Returns nil on success. On failure the caller should close the connection.
func ClientAuth(rw AuthIO, password string) error {
	header, err := readMsgWithTimeout(rw, authTimeout)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if string(header) != "SBRT-AUTH-1" {
		return fmt.Errorf("unexpected protocol header: %q", string(header))
	}

	saltSHexB, err := readMsgWithTimeout(rw, authTimeout)
	if err != nil {
		return fmt.Errorf("read server salt: %w", err)
	}
	saltS, err := hex.DecodeString(string(saltSHexB))
	if err != nil || len(saltS) != 32 {
		return fmt.Errorf("invalid server salt")
	}

	saltC, err := generateSalt()
	if err != nil {
		return fmt.Errorf("generate client salt: %w", err)
	}

	if err := rw.WriteMsg([]byte(hex.EncodeToString(saltC))); err != nil {
		return fmt.Errorf("write client salt: %w", err)
	}
	if err := rw.WriteMsg([]byte(hex.EncodeToString(computeHMAC(password, saltS)))); err != nil {
		return fmt.Errorf("write client HMAC: %w", err)
	}

	status, err := readMsgWithTimeout(rw, authTimeout)
	if err != nil {
		return fmt.Errorf("read server status: %w", err)
	}
	if string(status) == "FAIL" {
		return fmt.Errorf("rejected by server")
	}
	if string(status) != "OK" {
		return fmt.Errorf("unexpected status: %q", string(status))
	}

	serverHMACHexB, err := readMsgWithTimeout(rw, authTimeout)
	if err != nil {
		return fmt.Errorf("read server HMAC: %w", err)
	}
	serverHMACBytes, err := hex.DecodeString(string(serverHMACHexB))
	if err != nil {
		return fmt.Errorf("invalid server HMAC hex")
	}

	expected := computeHMAC(password, saltC)
	if !hmac.Equal(serverHMACBytes, expected) {
		return fmt.Errorf("server has wrong password")
	}

	return nil
}
