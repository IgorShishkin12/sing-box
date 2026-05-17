package reticulum

import "errors"

var (
	ErrBridgeInitFailed         = errors.New("bridge init failed")
	ErrBridgeDialFailed         = errors.New("bridge dial failed")
	ErrBridgeListenFailed       = errors.New("bridge listen failed")
	ErrBridgeAcceptFailed       = errors.New("bridge accept failed")
	ErrBridgePollFailed         = errors.New("bridge poll failed")
	ErrBridgeGetHashFailed      = errors.New("bridge get hash failed")
	ErrBridgeRegisterNameFailed = errors.New("bridge register name failed")
	ErrBridgeResolveNameFailed  = errors.New("bridge resolve name failed")
)
