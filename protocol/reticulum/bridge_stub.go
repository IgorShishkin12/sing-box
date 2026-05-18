//go:build !with_reticulum

package reticulum

import "errors"

var ErrBridgeNotAvailable = errors.New("reticulum bridge not available: build with -tags with_reticulum")

func BridgeInit(_ string) error { return ErrBridgeNotAvailable }

func BridgeListen(_ string) (uint64, error) { return 0, ErrBridgeNotAvailable }

// BridgeDial returns a task ID, a result channel, and an error.
// The channel will never receive in stub mode.
func BridgeDial(_ string) (uint64, <-chan uint64, error) {
	ch := make(chan uint64, 1)
	return 0, ch, ErrBridgeNotAvailable
}

func BridgeWrite(_ uint64, _ []byte) int { return -1 }

func BridgeClose(_ uint64) {}

func BridgeShutdown() {}

func BridgeResolveName(_ string) (string, error) { return "", ErrBridgeNotAvailable }
