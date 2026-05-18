// Package reticulum implements a mutual password authentication handshake for
// Reticulum bridge connections.
//
// Protocol (server speaks first):
//
//	Server → Client: "SBRT-AUTH-1\n" + hex(salt_s, 32 bytes) + "\n"
//	Client → Server: hex(salt_c, 32 bytes) + "\n" + hex(HMAC-SHA256(password, salt_s)) + "\n"
//	Server verifies; if wrong → returns error (caller should close the connection).
//	Server → Client: "OK\n" + hex(HMAC-SHA256(password, salt_c)) + "\n"
//	Client verifies; if wrong → returns error.
package reticulum

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
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

func writeLine(w io.Writer, line string) error {
	_, err := fmt.Fprintf(w, "%s\n", line)
	return err
}

// readLine reads one '\n'-terminated line with a fixed timeout.
func readLine(r *bufio.Reader) (string, error) {
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := r.ReadString('\n')
		if err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{line: strings.TrimRight(line, "\r\n")}
	}()
	select {
	case res := <-ch:
		return res.line, res.err
	case <-time.After(authTimeout):
		return "", fmt.Errorf("auth timeout")
	}
}

// ServerAuth runs the server side of the mutual password authentication.
//
// It generates a random salt, sends it to the client together with the
// protocol header, reads the client's salt and HMAC response, verifies
// the HMAC, and if correct sends back its own HMAC proof.
//
// Returns nil on success. On failure the caller should close the connection.
func ServerAuth(rw io.ReadWriter, password string) error {
	r := bufio.NewReader(rw)

	saltS, err := generateSalt()
	if err != nil {
		return fmt.Errorf("auth: generate server salt: %w", err)
	}

	if err := writeLine(rw, "SBRT-AUTH-1"); err != nil {
		return fmt.Errorf("auth: write header: %w", err)
	}
	if err := writeLine(rw, hex.EncodeToString(saltS)); err != nil {
		return fmt.Errorf("auth: write server salt: %w", err)
	}

	saltCHex, err := readLine(r)
	if err != nil {
		return fmt.Errorf("auth: read client salt: %w", err)
	}
	saltC, err := hex.DecodeString(saltCHex)
	if err != nil || len(saltC) != 32 {
		return fmt.Errorf("auth: invalid client salt")
	}

	clientHMACHex, err := readLine(r)
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

	if err := writeLine(rw, "OK"); err != nil {
		return fmt.Errorf("auth: write OK: %w", err)
	}
	serverHMAC := computeHMAC(password, saltC)
	if err := writeLine(rw, hex.EncodeToString(serverHMAC)); err != nil {
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
func ClientAuth(rw io.ReadWriter, password string) error {
	r := bufio.NewReader(rw)

	header, err := readLine(r)
	if err != nil {
		return fmt.Errorf("auth: read header: %w", err)
	}
	if header != "SBRT-AUTH-1" {
		return fmt.Errorf("auth: unexpected protocol header: %q", header)
	}

	saltSHex, err := readLine(r)
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
	if err := writeLine(rw, hex.EncodeToString(saltC)); err != nil {
		return fmt.Errorf("auth: write client salt: %w", err)
	}
	if err := writeLine(rw, hex.EncodeToString(clientHMAC)); err != nil {
		return fmt.Errorf("auth: write client HMAC: %w", err)
	}

	status, err := readLine(r)
	if err != nil {
		return fmt.Errorf("auth: read server status: %w", err)
	}
	if status != "OK" {
		return fmt.Errorf("auth: rejected by server")
	}

	serverHMACHex, err := readLine(r)
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
