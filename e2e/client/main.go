package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	reticulum "github.com/sagernet/sing-box/protocol/reticulum"
)

type sumRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type sumResponse struct {
	Sum int `json:"sum"`
}

func main() {
	// Build reticulum config for the client
	// Connects to the server's UDP interface at e2e-server:4242
	configJSON := `{
		"identity_name": "e2e-client",
		"storage_path": "/tmp/reticulum-client",
		"interfaces": [{"type": "udp 0.0.0.0 4243 e2e-server 4242"}]
	}`

	// Initialize bridge with config
	if err := reticulum.BridgeInit(configJSON); err != nil {
		log.Fatalf("BridgeInit failed: %v", err)
	}
	defer reticulum.BridgeShutdown()

	// Dial the server's hash
	destHash := "e2e-sum-server"
	if h := os.Getenv("DEST_HASH"); h != "" {
		destHash = h
	}

	taskID, err := reticulum.BridgeDial(destHash)
	if err != nil {
		log.Fatalf("BridgeDial failed: %v", err)
	}

	connHdl, err := reticulum.BridgePollTask(taskID, 30*time.Second)
	if err != nil {
		log.Fatalf("dial poll failed: %v", err)
	}
	log.Printf("connected to %q (handle=%d)", destHash, connHdl)
	defer reticulum.BridgeClose(connHdl)

	// Send HTTP request through the tunnel
	req := sumRequest{A: 3, B: 5}
	reqBody, _ := json.Marshal(req)
	httpReq := fmt.Sprintf("POST /sum HTTP/1.1\r\nHost: 127.0.0.1:8080\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(reqBody), reqBody)

	log.Printf("sending HTTP request: %s", string(reqBody))
	n := reticulum.BridgeWrite(connHdl, []byte(httpReq))
	if n != len(httpReq) {
		log.Fatalf("bridge write: expected %d, got %d", len(httpReq), n)
	}

	// Read HTTP response
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

	// Parse HTTP response
	bodyStart := indexOf(respBuf, "\r\n\r\n")
	if bodyStart < 0 {
		log.Fatalf("no body separator in response")
	}
	bodyStart += 4
	body := respBuf[bodyStart:]

	var resp sumResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Fatalf("failed to parse response JSON: %v (body: %s)", err, string(body))
	}

	expected := 8 // 3 + 5
	if resp.Sum != expected {
		log.Fatalf("expected sum %d, got %d", expected, resp.Sum)
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