//go:build with_reticulum

package reticulum

/*
#cgo CFLAGS: -I${SRCDIR}/../../bridge/include
#cgo !android LDFLAGS: ${SRCDIR}/../../bridge/target/release/libsing_box_reticulum_bridge.a -lpthread -ldl -lm
#cgo android,arm64 LDFLAGS: ${SRCDIR}/../../bridge/target/aarch64-linux-android/release/libsing_box_reticulum_bridge.a
#cgo android,arm   LDFLAGS: ${SRCDIR}/../../bridge/target/armv7-linux-androideabi/release/libsing_box_reticulum_bridge.a
#cgo android,386   LDFLAGS: ${SRCDIR}/../../bridge/target/i686-linux-android/release/libsing_box_reticulum_bridge.a
#cgo android,amd64 LDFLAGS: ${SRCDIR}/../../bridge/target/x86_64-linux-android/release/libsing_box_reticulum_bridge.a
#include "reticulum_bridge.h"

#include <stdlib.h>
extern void goOnLog    (uint8_t level, char* target, char* message);
*/
import "C"
import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/sagernet/sing-box/log"
)

var (
	bridgeInitOnce sync.Once
	bridgeInitErr  error
)

// bridgeLoggerVal holds the log.ContextLogger set by BridgeSetLogger.
var bridgeLoggerVal atomic.Value // stores log.ContextLogger



//export goOnLog
func goOnLog(level C.uint8_t, target *C.char, message *C.char) {
	logger, _ := bridgeLoggerVal.Load().(log.ContextLogger)
	if logger == nil {
		return
	}
	tgt := C.GoString(target)
	msg := "[" + tgt + "] " + C.GoString(message)
	switch uint8(level) {
	case 1:
		logger.Error(msg)
	case 2:
		logger.Warn(msg)
	case 3:
		logger.Info(msg)
	case 4:
		logger.Debug(msg)
	default: // 5 = Trace
		logger.Trace(msg)
	}
}


// BridgeSetLogger wires the Go log.ContextLogger into the Rust log callback.
// Call before BridgeInit to capture early initialisation events.
func BridgeSetLogger(logger log.ContextLogger) {
	bridgeLoggerVal.Store(logger)
	C.reticulum_set_log_callback(C.reticulum_log_fn(C.goOnLog))
}


// BridgeInit initializes the Rust bridge with a JSON config string.
// Only the first call crosses the CGO boundary; subsequent callers get the
// same result immediately. The first caller's config wins.
func BridgeInit(configJSON string) error {
	bridgeInitOnce.Do(func() {
		cstr := C.CString(configJSON)
		defer C.free(unsafe.Pointer(cstr))
		if C.reticulum_init(cstr) != 0 {
			bridgeInitErr = ErrBridgeInitFailed
		}
	})
	return bridgeInitErr
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


// BridgeGetListenerHash gets the address hash of a listener as a hex string.
// Returns the hash string, or an error if the listener is not found.
func BridgeGetListenerHash(listenerHandle uint64) (string, error) {
	hashStr := C.reticulum_get_listener_hash(C.uint64_t(listenerHandle))
	if hashStr == nil {
		return "", ErrBridgeGetHashFailed
	}
	defer C.reticulum_free(unsafe.Pointer(hashStr))
	return C.GoString(hashStr), nil
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
		// Error: read the error message from C
		if lenOut > 0 && resultOut != nil {
			errMsg := string(C.GoBytes(unsafe.Pointer(resultOut), C.int(lenOut)))
			C.reticulum_free(unsafe.Pointer(resultOut))
			return false, nil, errors.New(errMsg)
		}
		return false, nil, ErrBridgePollFailed
	}
}

// BridgeGetHash gets the destination hash for a given name.
// Returns the hash string, or an error if the name is unknown.
func BridgeGetHash(name string) (string, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var hashOut *C.char
	ret := C.get_hash(&hashOut, cname)
	if ret != 0 {
		return "", ErrBridgeGetHashFailed
	}
	if hashOut == nil {
		return "", ErrBridgeGetHashFailed
	}
	hashStr := C.GoString(hashOut)
	C.reticulum_free(unsafe.Pointer(hashOut))
	return hashStr, nil
}

// BridgeRegisterName registers a name→hash mapping for later lookup via BridgeGetHash.
func BridgeRegisterName(name string, hash string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	chash := C.CString(hash)
	defer C.free(unsafe.Pointer(chash))

	ret := C.reticulum_register_name(cname, chash)
	if ret != 0 {
		return ErrBridgeRegisterNameFailed
	}
	return nil
}

// BridgeShutdown shuts down the bridge.
func BridgeShutdown() {
	C.reticulum_shutdown()
}

// BridgeConnPeerHash returns the Reticulum identity hash of the remote peer for a
// connection handle. Only available on accepted (inbound) connections; returns an
// error for outbound connections or unknown handles.
func BridgeConnPeerHash(connHandle uint64) (string, error) {
	hashStr := C.reticulum_get_conn_peer_hash(C.uint64_t(connHandle))
	if hashStr == nil {
		return "", ErrBridgeConnPeerHashFailed
	}
	defer C.reticulum_free(unsafe.Pointer(hashStr))
	return C.GoString(hashStr), nil
}

// BridgeResolveName resolves a human-readable name to a deterministic address hash.
// Both listener and dialer can call this independently to get the same hash
// from the same name, without any shared state or network communication.
// Returns the 32-char hex address hash, or an error if resolution fails.
func BridgeResolveName(name string) (string, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	hashStr := C.reticulum_resolve_name(cname)
	if hashStr == nil {
		return "", ErrBridgeResolveNameFailed
	}
	defer C.reticulum_free(unsafe.Pointer(hashStr))
	return C.GoString(hashStr), nil
}

