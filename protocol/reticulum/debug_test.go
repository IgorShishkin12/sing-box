//go:build with_reticulum

package reticulum

import (
"fmt"
"testing"
)

func TestDebug(t *testing.T) {
	err := BridgeInit("")
	fmt.Printf("BridgeInit error: %v\n", err)
	if err != nil {
		t.Fatalf("BridgeInit failed: %v", err)
	}
	BridgeShutdown()
}
