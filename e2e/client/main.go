package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sagernet/sing-box/e2e/auth"
	"github.com/sagernet/sing-box/e2e/knownhosts"
	reticulum "github.com/sagernet/sing-box/protocol/reticulum"
)

// bridgeConn wraps a bridge connection handle as an io.ReadWriter.
type bridgeConn struct {
	handle uint64
}

func (c bridgeConn) Read(p []byte) (int, error) {
	for {
		n := reticulum.BridgeRead(c.handle, p)
		if n < 0 {
			return 0, io.EOF
		}
		if n > 0 {
			return n, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c bridgeConn) Write(p []byte) (int, error) {
	n := reticulum.BridgeWrite(c.handle, p)
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

type sumRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type sumResponse struct {
	Sum int `json:"sum"`
}

const configDir = "/tmp/reticulum-client"

func main() {
	// Hard timeout: fail fast rather than hang in CI.
	go func() {
		time.Sleep(120 * time.Second)
		log.Fatal("E2E TEST TIMEOUT: did not complete within 120s")
	}()

	password := os.Getenv("RETICULUM_PASSWORD")
	if password == "" {
		log.Fatal("RETICULUM_PASSWORD env var must be set")
	}

	configJSON := fmt.Sprintf(`{
		"identity_name": "e2e-client",
		"storage_path": %q,
		"interfaces": [
			{"name": "E2E UDP", "type": "UDPInterface",
			 "listen_ip": "0.0.0.0", "listen_port": 4242,
			 "forward_ip": "e2e-server", "forward_port": 4242}
		]
	}`, configDir)

	if err := reticulum.BridgeInit(configJSON); err != nil {
		log.Fatalf("BridgeInit failed: %v", err)
	}
	defer reticulum.BridgeShutdown()

	// Give the server container time to initialize, register destinations,
	// and start its first announce cycle.  podman-compose 1.0.6 starts both
	// containers simultaneously without waiting for the server healthcheck.
	log.Printf("waiting 5s for server to initialize...")
	time.Sleep(5 * time.Second)

	serverName := "e2e-sum-server"

	// Always do network discovery: this knocks the server's discovery
	// destination, which triggers it to announce its service hash. Without
	// the knock the server never announces, so dial_and_wait can't find the
	// destination identity in the announce table even if we have a cached hash.
	destHash := resolveAndCache(serverName)

	connHdl := dialWithRetry(serverName, destHash)
	defer reticulum.BridgeClose(connHdl)

	conn := bridgeConn{handle: connHdl}
	if err := auth.ClientAuth(conn, password); err != nil {
		log.Fatalf("auth failed: %v", err)
	}
	log.Printf("auth OK (handle=%d)", connHdl)

	sendAndVerify(connHdl)
}

// resolveAndCache always calls BridgeResolveName to knock the server's discovery
// destination. This triggers the server to announce its service identity, which
// is required before dial_and_wait can establish a link (it needs the identity
// in the announce table). After resolving, the hash is saved to known-hosts.
func resolveAndCache(serverName string) string {
	if hosts, err := knownhosts.Load(configDir); err == nil {
		if hash, ok := hosts[serverName]; ok {
			log.Printf("known-hosts cache has hash for %q: %s (will verify via discovery)", serverName, hash)
		}
	}

	log.Printf("resolving name %q via network discovery (mandatory for announce table)...", serverName)
	destHash, err := reticulum.BridgeResolveName(serverName)
	if err != nil {
		log.Fatalf("BridgeResolveName failed: %v", err)
	}
	log.Printf("resolved %q -> %s", serverName, destHash)

	if err := knownhosts.Save(configDir, serverName, destHash); err != nil {
		log.Printf("knownhosts save error (ignored): %v", err)
	}
	return destHash
}

// dialWithRetry dials destHash with retries. Falls back to re-resolving if all attempts fail.
func dialWithRetry(serverName, destHash string) uint64 {
	for i := 0; i < 15; i++ {
		taskID, err := reticulum.BridgeDial(destHash)
		if err != nil {
			log.Printf("BridgeDial attempt %d: %v", i+1, err)
			time.Sleep(2 * time.Second)
			continue
		}
		connHdl, err := reticulum.BridgePollTask(taskID, 30*time.Second)
		if err == nil {
			log.Printf("connected to %q (handle=%d)", serverName, connHdl)
			return connHdl
		}
		log.Printf("dial poll attempt %d: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("failed to dial %q after retries", serverName)
	return 0
}

func sendAndVerify(connHdl uint64) {
	req := sumRequest{A: 3, B: 5}
	reqBody, _ := json.Marshal(req)
	httpReq := fmt.Sprintf("POST /sum HTTP/1.1\r\nHost: 127.0.0.1:8080\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(reqBody), reqBody)

	log.Printf("sending HTTP request: %s", string(reqBody))
	n := reticulum.BridgeWrite(connHdl, []byte(httpReq))
	if n != len(httpReq) {
		log.Fatalf("bridge write: expected %d, got %d", len(httpReq), n)
	}

	// Read until we have the full HTTP headers (ending with \r\n\r\n).
	tmp := make([]byte, 4096)
	var rawBuf []byte
	for indexOf(rawBuf, "\r\n\r\n") < 0 {
		n := reticulum.BridgeRead(connHdl, tmp)
		if n < 0 {
			log.Fatalf("bridge read error while reading response headers")
		}
		if n == 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		rawBuf = append(rawBuf, tmp[:n]...)
	}

	sepIdx := indexOf(rawBuf, "\r\n\r\n")
	headers := string(rawBuf[:sepIdx])
	body := rawBuf[sepIdx+4:]

	// Parse Content-Length so we know exactly when the body is complete.
	contentLength := -1
	for _, line := range splitLines(headers) {
		if len(line) > 15 && strings.EqualFold(line[:15], "content-length:") {
			fmt.Sscanf(strings.TrimSpace(line[15:]), "%d", &contentLength)
		}
	}
	if contentLength < 0 {
		log.Fatalf("no Content-Length in response headers")
	}

	for len(body) < contentLength {
		n := reticulum.BridgeRead(connHdl, tmp)
		if n < 0 {
			break
		}
		if n == 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		body = append(body, tmp[:n]...)
	}
	body = body[:contentLength]

	log.Printf("received response headers:\n%s", headers)
	log.Printf("received response body (%d bytes): %s", len(body), string(body))

	var resp sumResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Fatalf("failed to parse response JSON: %v (body: %s)", err, string(body))
	}

	if resp.Sum != 8 {
		log.Fatalf("expected sum 8, got %d", resp.Sum)
	}

	fmt.Printf("E2E TEST PASSED: 3 + 5 = %d\n", resp.Sum)
}

func splitLines(s string) []string {
	return strings.Split(s, "\r\n")
}

func indexOf(b []byte, s string) int {
	for i := 0; i <= len(b)-len(s); i++ {
		if string(b[i:i+len(s)]) == s {
			return i
		}
	}
	return -1
}
