package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
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
	n := reticulum.BridgeRead(c.handle, p)
	if n < 0 {
		return 0, io.EOF
	}
	return n, nil
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
	password := os.Getenv("RETICULUM_PASSWORD")
	if password == "" {
		log.Fatal("RETICULUM_PASSWORD env var must be set")
	}

	configJSON := fmt.Sprintf(`{
		"identity_name": "e2e-client",
		"storage_path": %q,
		"interfaces": [{"type": "udp 0.0.0.0 4243 e2e-server 4242"}]
	}`, configDir)

	if err := reticulum.BridgeInit(configJSON); err != nil {
		log.Fatalf("BridgeInit failed: %v", err)
	}
	defer reticulum.BridgeShutdown()

	serverName := "e2e-sum-server"

	// Check known-hosts cache first.
	destHash := cachedOrResolve(serverName)

	connHdl := dialWithRetry(serverName, destHash)
	defer reticulum.BridgeClose(connHdl)

	conn := bridgeConn{handle: connHdl}
	if err := auth.ClientAuth(conn, password); err != nil {
		log.Fatalf("auth failed: %v", err)
	}
	log.Printf("auth OK (handle=%d)", connHdl)

	sendAndVerify(connHdl)
}

// cachedOrResolve returns the service hash from the known-hosts cache if available,
// otherwise does network discovery.
func cachedOrResolve(serverName string) string {
	hosts, err := knownhosts.Load(configDir)
	if err != nil {
		log.Printf("knownhosts load error (ignored): %v", err)
	}
	if hash, ok := hosts[serverName]; ok {
		log.Printf("using cached hash for %q: %s", serverName, hash)
		return hash
	}

	log.Printf("resolving name %q via network discovery...", serverName)
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

	var respBuf []byte
	tmp := make([]byte, 65536)
	for {
		n := reticulum.BridgeRead(connHdl, tmp)
		if n <= 0 {
			break
		}
		respBuf = append(respBuf, tmp[:n]...)
		if len(respBuf) >= 4 && string(respBuf[len(respBuf)-4:]) == "\r\n\r\n" {
			break
		}
	}

	log.Printf("received response (%d bytes): %s", len(respBuf), string(respBuf))

	bodyStart := indexOf(respBuf, "\r\n\r\n")
	if bodyStart < 0 {
		log.Fatalf("no body separator in response")
	}
	body := respBuf[bodyStart+4:]

	var resp sumResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Fatalf("failed to parse response JSON: %v (body: %s)", err, string(body))
	}

	if resp.Sum != 8 {
		log.Fatalf("expected sum 8, got %d", resp.Sum)
	}

	fmt.Printf("E2E TEST PASSED: 3 + 5 = %d\n", resp.Sum)
}

func indexOf(b []byte, s string) int {
	for i := 0; i <= len(b)-len(s); i++ {
		if string(b[i:i+len(s)]) == s {
			return i
		}
	}
	return -1
}
