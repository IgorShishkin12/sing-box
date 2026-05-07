package e2e

import (
	"testing"
	"time"

	reticulum "github.com/sagernet/sing-box/protocol/reticulum"
)

func TestE2EReticulumPairedConnection(t *testing.T) {
	// Initialize the bridge
	err := reticulum.BridgeInit("")
	if err != nil {
		t.Fatalf("BridgeInit failed: %v", err)
	}
	defer reticulum.BridgeShutdown()

	// Listen on a hash
	listenHash := "test-echo"
	listenTaskID, err := reticulum.BridgeListen(listenHash)
	if err != nil {
		t.Fatalf("BridgeListen failed: %v", err)
	}
	if listenTaskID <= 0 {
		t.Fatalf("expected positive listen task ID, got %d", listenTaskID)
	}

	// Poll for listener handle
	listenerHandle, err := reticulum.BridgePollTask(listenTaskID, 5*time.Second)
	if err != nil {
		t.Fatalf("listen poll failed: %v", err)
	}
	if listenerHandle == 0 {
		t.Fatal("listener handle is zero")
	}

	// Dial the same hash — should result in a paired connection
	dialTaskID, err := reticulum.BridgeDial(listenHash)
	if err != nil {
		t.Fatalf("BridgeDial failed: %v", err)
	}
	if dialTaskID <= 0 {
		t.Fatalf("expected positive dial task ID, got %d", dialTaskID)
	}

	// Poll for dial completion
	dialHandle, err := reticulum.BridgePollTask(dialTaskID, 5*time.Second)
	if err != nil {
		t.Fatalf("dial poll failed: %v", err)
	}
	if dialHandle == 0 {
		t.Fatal("dial handle is zero")
	}

	// Accept the incoming connection from the listener
	acceptTaskID, err := reticulum.BridgeAccept(listenerHandle)
	if err != nil {
		t.Fatalf("BridgeAccept failed: %v", err)
	}
	if acceptTaskID <= 0 {
		t.Fatalf("expected positive accept task ID, got %d", acceptTaskID)
	}

	acceptHandle, err := reticulum.BridgePollTask(acceptTaskID, 5*time.Second)
	if err != nil {
		t.Fatalf("accept poll failed: %v", err)
	}
	if acceptHandle == 0 {
		t.Fatal("accept handle is zero")
	}

	// Write data from dial side
	writeData := []byte("hello e2e")
	n := reticulum.BridgeWrite(dialHandle, writeData)
	if n != len(writeData) {
		t.Fatalf("wrote %d bytes, expected %d", n, len(writeData))
	}

	// Read data on accept side — should get what dial side wrote
	readBuf := make([]byte, 1024)
	n = reticulum.BridgeRead(acceptHandle, readBuf)
	if n != len(writeData) {
		t.Fatalf("read %d bytes, expected %d", n, len(writeData))
	}
	got := readBuf[:n]
	if string(got) != string(writeData) {
		t.Fatalf("read %q, expected %q", got, writeData)
	}

	// Write a response back from accept side
	responseData := []byte("echo: hello e2e")
	n = reticulum.BridgeWrite(acceptHandle, responseData)
	if n != len(responseData) {
		t.Fatalf("wrote %d bytes, expected %d", n, len(responseData))
	}

	// Read the response on dial side
	n = reticulum.BridgeRead(dialHandle, readBuf)
	if n != len(responseData) {
		t.Fatalf("read %d bytes, expected %d", n, len(responseData))
	}
	got = readBuf[:n]
	if string(got) != string(responseData) {
		t.Fatalf("read %q, expected %q", got, responseData)
	}

	// Cleanup
	reticulum.BridgeClose(dialHandle)
	reticulum.BridgeClose(acceptHandle)
	reticulum.BridgeClose(listenerHandle)
}