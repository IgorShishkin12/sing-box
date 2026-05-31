//go:build !with_reticulum

package reticulum

import (
	"errors"

	"github.com/sagernet/sing-box/log"
)

func BridgeSetLogger(_ log.ContextLogger) {}

func BridgeInit(configJSON string) error { return ErrBridgeNotAvailable }

// BridgeDial stub — returns error immediately via resultCh.
func BridgeDial(destinationHash string) (uint64, <-chan uint64, error) {
	return 0, nil, ErrBridgeNotAvailable
}

// BridgeListen stub.
func BridgeListen(listenHash string) (uint64, error) {
	return 0, ErrBridgeNotAvailable
}

func BridgeGetListenerHash(listenerHandle uint64) (string, error) {
	return "", ErrBridgeNotAvailable
}

func BridgeClose(handle uint64) {}

func BridgeWrite(connHandle uint64, data []byte) int { return -1 }

func BridgeGetHash(name string) (string, error)                  { return "", ErrBridgeNotAvailable }
func BridgeRegisterName(name string, hash string) error          { return ErrBridgeNotAvailable }
func BridgeShutdown()                                             {}
func BridgeResolveName(name string) (string, error)              { return "", ErrBridgeNotAvailable }
func BridgeConnIdentifiedPeer(connHandle uint64) (string, error) { return "", ErrBridgeNotAvailable }
func BridgeConnPeerHash(connHandle uint64) (string, error)       { return "", ErrBridgeNotAvailable }
func BridgeTransportHash() (string, error)                       { return "", ErrBridgeNotAvailable }

var ErrBridgeNotAvailable = errors.New("reticulum bridge not available: build with -tags with_reticulum")
