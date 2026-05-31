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
extern void goOnLog    (uint8_t level,      char*     target,    char*     message);
extern void goOnAccept (uint64_t listener_id, uint64_t conn_id, char*     peer_hash);
extern void goOnConnect(uint64_t task_id,     uint64_t conn_id);
extern void goOnData   (uint64_t conn_id,     uint8_t* data,    size_t    len);
extern void goOnClose  (uint64_t conn_id);
*/
import "C"
import (
	"sync"
	"unsafe"

	"github.com/sagernet/sing-box/log"
)

var (
	bridgeInitOnce sync.Once
	bridgeInitErr  error
)

var bridgeLoggerVal interface{ Error(args ...interface{}); Warn(args ...interface{}); Info(args ...interface{}); Debug(args ...interface{}); Trace(args ...interface{}) }

// ---------------------------------------------------------------------------
// Log callback
// ---------------------------------------------------------------------------

//export goOnLog
func goOnLog(level C.uint8_t, target *C.char, message *C.char) {
	logger := bridgeLoggerVal
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
	default:
		logger.Trace(msg)
	}
}

// ---------------------------------------------------------------------------
// Async callbacks (called from Rust tokio threads)
// ---------------------------------------------------------------------------

//export goOnAccept
func goOnAccept(listenerID C.uint64_t, connID C.uint64_t, peerHash *C.char) {
	ev := acceptEvent{
		listenerID: uint64(listenerID),
		connID:     uint64(connID),
		peerHash:   C.GoString(peerHash),
	}
	select {
	case globalAcceptCh <- ev:
	default:
		// Channel full; drop. The listener acceptLoop is not keeping up.
	}
}

//export goOnConnect
func goOnConnect(taskID C.uint64_t, connID C.uint64_t) {
	if ch, ok := pendingDials.LoadAndDelete(uint64(taskID)); ok {
		ch.(chan uint64) <- uint64(connID)
	}
}

//export goOnData
func goOnData(connID C.uint64_t, data *C.uint8_t, length C.size_t) {
	id := uint64(connID)
	// Auto-create an entry if the connection isn't registered yet — this handles
	// the race where data arrives (e.g. auth bytes) before newReticulumConn is
	// called. UDP has lower latency so this window is more likely to be hit.
	actual, _ := connDataChans.LoadOrStore(id, &connEntry{
		ch:   make(chan []byte, 256),
		done: make(chan struct{}),
	})
	entry := actual.(*connEntry)
	buf := make([]byte, int(length))
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(data)), int(length)))
	select {
	case entry.ch <- buf:
	case <-entry.done:
		// connection already closed
	default:
		// channel buffer full — drop packet
	}
}

//export goOnClose
func goOnClose(connID C.uint64_t) {
	id := uint64(connID)
	if v, ok := connDataChans.LoadAndDelete(id); ok {
		entry := v.(*connEntry)
		entry.once.Do(func() { close(entry.done) })
	}
}

// ---------------------------------------------------------------------------
// Bridge API
// ---------------------------------------------------------------------------

// BridgeSetLogger wires the Go log.ContextLogger into the Rust log callback.
// Call before BridgeInit.
func BridgeSetLogger(logger log.ContextLogger) {
	bridgeLoggerVal = logger
	C.reticulum_set_log_callback(C.reticulum_log_fn(C.goOnLog))
}

// BridgeInit initializes the Rust bridge. Only the first call crosses CGO.
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

// BridgeDial fires an async dial. Returns (taskID, resultCh, err).
// Wait on resultCh for the conn_id; 0 means failure.
func BridgeDial(destinationHash string) (uint64, <-chan uint64, error) {
	cstr := C.CString(destinationHash)
	defer C.free(unsafe.Pointer(cstr))

	nextDialMu.Lock()
	nextDialTaskSeq++
	taskID := nextDialTaskSeq
	nextDialMu.Unlock()

	resultCh := make(chan uint64, 1)
	pendingDials.Store(taskID, resultCh)
	C.reticulum_dial(C.uint64_t(taskID), cstr)
	return taskID, resultCh, nil
}

// BridgeListen registers a listener synchronously.
// Returns the listener handle on success, or an error.
func BridgeListen(listenHash string) (uint64, error) {
	cstr := C.CString(listenHash)
	defer C.free(unsafe.Pointer(cstr))
	handle := int64(C.reticulum_listen(cstr))
	if handle < 0 {
		return 0, ErrBridgeListenFailed
	}
	return uint64(handle), nil
}

// BridgeGetListenerHash returns the address hash of a listener.
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

// BridgeGetHash gets the destination hash for a registered name.
func BridgeGetHash(name string) (string, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var hashOut *C.char
	if C.get_hash(&hashOut, cname) != 0 || hashOut == nil {
		return "", ErrBridgeGetHashFailed
	}
	s := C.GoString(hashOut)
	C.reticulum_free(unsafe.Pointer(hashOut))
	return s, nil
}

// BridgeRegisterName registers a name→hash mapping.
func BridgeRegisterName(name string, hash string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	chash := C.CString(hash)
	defer C.free(unsafe.Pointer(chash))
	if C.reticulum_register_name(cname, chash) != 0 {
		return ErrBridgeRegisterNameFailed
	}
	return nil
}

// BridgeShutdown shuts down the bridge.
func BridgeShutdown() { C.reticulum_shutdown() }

// BridgeResolveName resolves a human-readable name to an address hash.
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

// BridgeConnIdentifiedPeer returns the verified persistent identity hash of the remote peer.
func BridgeConnIdentifiedPeer(connHandle uint64) (string, error) {
	hashStr := C.reticulum_get_conn_identified_peer(C.uint64_t(connHandle))
	if hashStr == nil {
		return "", ErrBridgeConnIdentifyFailed
	}
	defer C.reticulum_free(unsafe.Pointer(hashStr))
	return C.GoString(hashStr), nil
}

// BridgeConnPeerHash returns the peer's link identity hash for a connection.
func BridgeConnPeerHash(connHandle uint64) (string, error) {
	hashStr := C.reticulum_get_conn_peer_hash(C.uint64_t(connHandle))
	if hashStr == nil {
		return "", ErrBridgeConnPeerHashFailed
	}
	defer C.reticulum_free(unsafe.Pointer(hashStr))
	return C.GoString(hashStr), nil
}

// BridgeTransportHash returns the local transport identity address hash.
func BridgeTransportHash() (string, error) {
	hashStr := C.reticulum_get_transport_hash()
	if hashStr == nil {
		return "", ErrBridgeTransportHashFailed
	}
	defer C.reticulum_free(unsafe.Pointer(hashStr))
	return C.GoString(hashStr), nil
}

// nextDialMu protects nextDialTaskSeq.
var nextDialMu sync.Mutex
