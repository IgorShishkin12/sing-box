package main

import (
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing-box/e2e/auth"
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

func main() {
	password := os.Getenv("RETICULUM_PASSWORD")
	if password == "" {
		log.Fatal("RETICULUM_PASSWORD env var must be set")
	}

	configJSON := `{
		"identity_name": "e2e-server",
		"storage_path": "/tmp/reticulum-server",
		"interfaces": [{"type": "udp 0.0.0.0 4242"}]
	}`

	if err := reticulum.BridgeInit(configJSON); err != nil {
		log.Fatalf("BridgeInit failed: %v", err)
	}
	defer reticulum.BridgeShutdown()

	listenName := "e2e-sum-server"

	forwardAddr := "127.0.0.1:8080"
	if f := os.Getenv("FORWARD_ADDR"); f != "" {
		forwardAddr = f
	}

	taskID, err := reticulum.BridgeListen(listenName)
	if err != nil {
		log.Fatalf("BridgeListen failed: %v", err)
	}

	listenerHdl, err := reticulum.BridgePollTask(taskID, 30*time.Second)
	if err != nil {
		log.Fatalf("listen poll failed: %v", err)
	}
	log.Printf("listening on name %q (handle=%d), forwarding to %s", listenName, listenerHdl, forwardAddr)

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
		go handleConnection(connHdl, forwardAddr, password)
	}
}

func handleConnection(connHdl uint64, forwardAddr, password string) {
	defer reticulum.BridgeClose(connHdl)

	conn := bridgeConn{handle: connHdl}
	if err := auth.ServerAuth(conn, password); err != nil {
		log.Printf("auth failed (handle=%d): %v", connHdl, err)
		return
	}
	log.Printf("auth OK (handle=%d)", connHdl)

	target, err := net.DialTimeout("tcp", forwardAddr, 10*time.Second)
	if err != nil {
		log.Printf("failed to connect to %s: %v", forwardAddr, err)
		return
	}
	defer target.Close()

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
