//go:build !with_reticulum

package reticulum

import (
	"errors"

	"github.com/sagernet/sing-box/log"
)

func BridgeSetLogger(_ log.ContextLogger) {}

// BridgeInit is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeInit(configJSON string) error {
	return ErrBridgeNotAvailable
}

// BridgeDial is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeDial(destinationHash string) (int, error) {
	return -1, ErrBridgeNotAvailable
}

// BridgeListen is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeListen(listenHash string) (int, error) {
	return -1, ErrBridgeNotAvailable
}

// BridgeAccept is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeAccept(listenerHandle uint64) (int, error) {
	return -1, ErrBridgeNotAvailable
}

// BridgeGetListenerHash is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeGetListenerHash(listenerHandle uint64) (string, error) {
	return "", ErrBridgeNotAvailable
}

// BridgeClose is a no-op stub.
func BridgeClose(handle uint64) {}

// BridgeWrite is a stub that returns -1 when the with_reticulum build tag is not set.
func BridgeWrite(connHandle uint64, data []byte) int {
	return -1
}

// BridgeRead is a stub that returns -1 when the with_reticulum build tag is not set.
func BridgeRead(connHandle uint64, buffer []byte) int {
	return -1
}

// BridgePoll is a stub that returns an error when the with_reticulum build tag is not set.
func BridgePoll(taskID int) (done bool, result []byte, err error) {
	return false, nil, ErrBridgeNotAvailable
}

// BridgeGetHash is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeGetHash(name string) (string, error) {
	return "", ErrBridgeNotAvailable
}

// BridgeRegisterName is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeRegisterName(name string, hash string) error {
	return ErrBridgeNotAvailable
}

// BridgeShutdown is a no-op stub.
func BridgeShutdown() {}

// BridgeResolveName is a stub that returns an error when the with_reticulum build tag is not set.
func BridgeResolveName(name string) (string, error) {
	return "", ErrBridgeNotAvailable
}

var ErrBridgeNotAvailable = errors.New("reticulum bridge not available: build with -tags with_reticulum")
