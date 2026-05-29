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

// Forward-declare the Go-exported callback shims so we can pass them to
// reticulum_init without a cast.
extern void goOnAccept (uint64_t listener_id, uint64_t conn_id, char* peer_hash);
extern void goOnConnect(uint64_t task_id,     uint64_t conn_id);
extern void goOnData   (uint64_t conn_id,     uint8_t* data, size_t len);
extern void goOnClose  (uint64_t conn_id);
extern void goOnLog    (uint8_t level, char* target, char* message);
*/
import "C"
import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/sagernet/sing-box/log"
)

// ---------------------------------------------------------------------------
// Global event state
// ---------------------------------------------------------------------------

// pendingDials maps taskID → result channel (chan uint64, capacity 1).
// A zero connID in the channel means dial failure.
var pendingDials sync.Map // uint64 → chan uint64

// nextTaskSeq generates unique task IDs for outbound dials.
var nextTaskSeq atomic.Uint64

// ---------------------------------------------------------------------------
// Exported callbacks (called from Rust tokio threads)
// ---------------------------------------------------------------------------

//export goOnAccept
func goOnAccept(listenerID, connID C.uint64_t, peerHash *C.char) {
	// Pre-register the data channel immediately so that goOnData does not
	// drop packets that arrive before handleConn calls newReticulumConn.
	ch := make(chan []byte, 256)
	connDataChans.Store(uint64(connID), ch)
	hash := C.GoString(peerHash)
	ev := acceptEvent{
		listenerID: uint64(listenerID),
		connID:     uint64(connID),
		peerHash:   hash,
	}
	select {
	case globalAcceptCh <- ev:
	default:
		// Channel full — should not happen with capacity 256; drop and log.
	}
}

//export goOnConnect
func goOnConnect(taskID, connID C.uint64_t) {
	id := uint64(connID)
	if id != 0 {
		// Pre-register the data channel immediately so that goOnData does not
		// drop packets that arrive before DialContext calls newReticulumConn.
		ch := make(chan []byte, 256)
		connDataChans.Store(id, ch)
	}
	if ch, ok := pendingDials.LoadAndDelete(uint64(taskID)); ok {
		ch.(chan uint64) <- id
	}
}

//export goOnData
func goOnData(connID C.uint64_t, data *C.uint8_t, length C.size_t) {
	n := int(length)
	if n == 0 {
		return
	}
	buf := make([]byte, n)
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(data)), n))
	if ch, ok := connDataChans.Load(uint64(connID)); ok {
		select {
		case ch.(chan []byte) <- buf:
		default:
			// dataCh full — receiver is not keeping up; drop packet.
			if logger, _ := bridgeLoggerVal.Load().(log.ContextLogger); logger != nil {
				logger.Trace("[bridge] goOnData: dataCh full, dropped ", n, "B for conn ", uint64(connID))
			}
		}
	} else {
		// conn not yet registered (race window) or already closed.
		if logger, _ := bridgeLoggerVal.Load().(log.ContextLogger); logger != nil {
			logger.Trace("[bridge] goOnData: conn ", uint64(connID), " not registered, dropped ", n, "B")
		}
	}
}

//export goOnClose
func goOnClose(connID C.uint64_t) {
	if ch, ok := connDataChans.LoadAndDelete(uint64(connID)); ok {
		close(ch.(chan []byte))
	}
}

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

// ---------------------------------------------------------------------------
// Bridge API (called from Go)
// ---------------------------------------------------------------------------

var (
	bridgeInitOnce sync.Once
	bridgeInitErr  error
)

// BridgeSetLogger wires the Go log.ContextLogger into the Rust log callback.
// Call before BridgeInit to capture early initialisation events.
func BridgeSetLogger(logger log.ContextLogger) {
	bridgeLoggerVal.Store(logger)
	C.reticulum_set_log_callback(C.reticulum_log_fn(C.goOnLog))
}

// BridgeInit initializes the Rust bridge with a JSON config string.
// Registers the four Go callbacks. Only the first call matters.
func BridgeInit(configJSON string) error {
	bridgeInitOnce.Do(func() {
		cstr := C.CString(configJSON)
		defer C.free(unsafe.Pointer(cstr))
		ret := C.reticulum_init(
			cstr,
			C.reticulum_on_accept_fn(C.goOnAccept),
			C.reticulum_on_connect_fn(C.goOnConnect),
			C.reticulum_on_data_fn(C.goOnData),
			C.reticulum_on_close_fn(C.goOnClose),
		)
		if ret != 0 {
			bridgeInitErr = ErrBridgeInitFailed
		}
	})
	return bridgeInitErr
}

// BridgeListen registers a named service listener synchronously.
// Returns the listener handle on success.
func BridgeListen(name string) (uint64, error) {
	cstr := C.CString(name)
	defer C.free(unsafe.Pointer(cstr))
	id := C.reticulum_listen(cstr)
	if id < 0 {
		return 0, ErrBridgeListenFailed
	}
	return uint64(id), nil
}

// BridgeDial initiates a non-blocking dial. Returns a task ID and a
// channel that will receive the conn handle (0 = failure) when done.
func BridgeDial(destHash string) (uint64, <-chan uint64, error) {
	cstr := C.CString(destHash)
	defer C.free(unsafe.Pointer(cstr))

	taskID := nextTaskSeq.Add(1)
	resultCh := make(chan uint64, 1)
	pendingDials.Store(taskID, resultCh)

	C.reticulum_dial(C.uint64_t(taskID), cstr)
	return taskID, resultCh, nil
}

// BridgeWrite writes data to a connection.
func BridgeWrite(connHandle uint64, data []byte) int {
	if len(data) == 0 {
		return 0
	}
	n := C.reticulum_write(
		C.uint64_t(connHandle),
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	return int(n)
}

// BridgeClose closes a connection or listener handle.
func BridgeClose(handle uint64) {
	C.reticulum_close(C.uint64_t(handle))
}

// BridgeShutdown shuts down the bridge.
func BridgeShutdown() {
	C.reticulum_shutdown()
}

// BridgeResolveName resolves a human-readable name to its address hash.
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
