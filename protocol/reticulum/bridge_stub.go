package reticulum

/*
#cgo CFLAGS: -I${SRCDIR}/../../bridge/include
#cgo LDFLAGS: -L${SRCDIR}/../../bridge/target/debug -lsing_box_reticulum_bridge -lpthread -ldl -lm
#include "reticulum_bridge.h"
*/
import "C"
import (
	"errors"
	"unsafe"
)

// BridgeInit initializes the Rust bridge with a JSON config string.
// Returns nil on success, or an error string.
// Passes NULL to reticulum_init when configJSON is empty (default config).
func BridgeInit(configJSON string) error {
	if configJSON == "" {
		ret := C.reticulum_init(nil)
		if ret != 0 {
			return ErrBridgeInitFailed
		}
		return nil
	}
	cstr := C.CString(configJSON)
	defer C.free(unsafe.Pointer(cstr))
	ret := C.reticulum_init(cstr)
	if ret != 0 {
		return ErrBridgeInitFailed
	}
	return nil
}

// BridgeDial calls reticulum_dial and returns a task ID.
// Use BridgePollTask to wait for completion and get the connection handle.
func BridgeDial(destinationHash string) (int, error) {
	cstr := C.CString(destinationHash)
	defer C.free(unsafe.Pointer(cstr))
	taskID := int(C.reticulum_dial(cstr))
	if taskID < 0 {
		return -1, ErrBridgeDialFailed
	}
	return taskID, nil
}

// BridgeListen calls reticulum_listen and returns a task ID.
// Use BridgePollTask to wait for completion and get the listener handle.
func BridgeListen(listenHash string) (int, error) {
	cstr := C.CString(listenHash)
	defer C.free(unsafe.Pointer(cstr))
	taskID := int(C.reticulum_listen(cstr))
	if taskID < 0 {
		return -1, ErrBridgeListenFailed
	}
	return taskID, nil
}

// BridgeAccept calls reticulum_accept and returns a task ID.
// Use BridgePollTask to wait for completion and get the new connection handle.
func BridgeAccept(listenerHandle uint64) (int, error) {
	taskID := int(C.reticulum_accept(C.uint64_t(listenerHandle)))
	if taskID < 0 {
		return -1, ErrBridgeAcceptFailed
	}
	return taskID, nil
}


// BridgeClose closes a handle.
func BridgeClose(handle uint64) {
	C.reticulum_close(C.uint64_t(handle))
}

// BridgeWrite writes data to a connection.
func BridgeWrite(connHandle uint64, data []byte) int {
	if len(data) == 0 {
		return 0
	}
	n := C.reticulum_write(C.uint64_t(connHandle), (*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	return int(n)
}

// BridgeRead reads data from a connection.
func BridgeRead(connHandle uint64, buffer []byte) int {
	if len(buffer) == 0 {
		return 0
	}
	n := C.reticulum_read(C.uint64_t(connHandle), (*C.uint8_t)(unsafe.Pointer(&buffer[0])), C.size_t(len(buffer)))
	return int(n)
}

// BridgePoll polls for task completion.
func BridgePoll(taskID int) (done bool, result []byte, err error) {
	var resultOut *C.uchar
	var lenOut C.size_t
	ret := C.reticulum_poll(C.int(taskID), (*unsafe.Pointer)(unsafe.Pointer(&resultOut)), &lenOut)
	switch ret {
	case 0:
		return false, nil, nil
	case 1:
		if lenOut > 0 {
			result = C.GoBytes(unsafe.Pointer(resultOut), C.int(lenOut))
			C.reticulum_free(unsafe.Pointer(resultOut))
		}
		return true, result, nil
	default:
		return false, nil, ErrBridgePollFailed
	}
}

// BridgeShutdown shuts down the bridge.
func BridgeShutdown() {
	C.reticulum_shutdown()
}

// Errors
var (
	ErrBridgeInitFailed  = errors.New("bridge init failed")
	ErrBridgeDialFailed  = errors.New("bridge dial failed")
	ErrBridgeListenFailed = errors.New("bridge listen failed")
	ErrBridgeAcceptFailed = errors.New("bridge accept failed")
	ErrBridgePollFailed  = errors.New("bridge poll failed")
)

