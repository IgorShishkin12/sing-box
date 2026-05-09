package main

import (
	"io"
	"log"
	"net"
	"os"
	"time"

	reticulum "github.com/sagernet/sing-box/protocol/reticulum"
)

func main() {
	// Build reticulum config for the server
	configJSON := `{
		"identity_name": "e2e-server",
		"storage_path": "/tmp/reticulum-server",
		"interfaces": [{"type": "udp 0.0.0.0 4242"}]
	}`

	// Initialize bridge with config
	if err := reticulum.BridgeInit(configJSON); err != nil {
		log.Fatalf("BridgeInit failed: %v", err)
	}
	defer reticulum.BridgeShutdown()

	// Listen on the destination hash
	destHash := "e2e-sum-server"
	if h := os.Getenv("DEST_HASH"); h != "" {
		destHash = h
	}

	forwardAddr := "127.0.0.1:8080"
	if f := os.Getenv("FORWARD_ADDR"); f != "" {
		forwardAddr = f
	}

	taskID, err := reticulum.BridgeListen(destHash)
	if err != nil {
		log.Fatalf("BridgeListen failed: %v", err)
	}

	listenerHdl, err := reticulum.BridgePollTask(taskID, 30*time.Second)
	if err != nil {
		log.Fatalf("listen poll failed: %v", err)
	}
	log.Printf("listening on hash %q (handle=%d), forwarding to %s", destHash, listenerHdl, forwardAddr)

	// Accept loop
	for {
		acceptTaskID, err := reticulum.BridgeAccept(listenerHdl)
		if err != nil {
			log.Printf("BridgeAccept failed: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		connHdl, err := reticulum.BridgePollTask(acceptTaskID, 30*time.Second)
		if err != nil {
			log.Printf("accept poll failed: %v", err)
			continue
		}

		log.Printf("accepted connection (handle=%d)", connHdl)
		go handleConnection(connHdl, forwardAddr)
	}
}

func handleConnection(connHdl uint64, forwardAddr string) {
	defer reticulum.BridgeClose(connHdl)

	// Connect to the forward target
	target, err := net.DialTimeout("tcp", forwardAddr, 10*time.Second)
	if err != nil {
		log.Printf("failed to connect to %s: %v", forwardAddr, err)
		return
	}
	defer target.Close()

	// Bridge connection → target
	go func() {
		buf := make([]byte, 65536)
		for {
			n := reticulum.BridgeRead(connHdl, buf)
			if n <= 0 {
				return
			}
			if _, err := target.Write(buf[:n]); err != nil {
				return
			}
		}
	}()

	// Target → bridge connection
	buf := make([]byte, 65536)
	for {
		n, err := target.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("target read error: %v", err)
			}
			return
		}
		written := reticulum.BridgeWrite(connHdl, buf[:n])
		if written != n {
			log.Printf("bridge write: expected %d, got %d", n, written)
			return
		}
	}
}