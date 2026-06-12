//go:build android && with_reticulum

package main

/*
#include <jni.h>
#include <android/log.h>
#include <stdlib.h>
#include "../../bridge/include/reticulum_bridge.h"

static void* extract_jvm(JNIEnv* env) {
    JavaVM* jvm = NULL;
    (*env)->GetJavaVM(env, &jvm);
    return (void*)jvm;
}
static const char* jni_get_utf(JNIEnv* env, jstring s) {
    return (*env)->GetStringUTFChars(env, s, NULL);
}
static void jni_release_utf(JNIEnv* env, jstring s, const char* c) {
    (*env)->ReleaseStringUTFChars(env, s, c);
}
static void do_log(int prio, const char* msg) {
    __android_log_print(prio, "sing-box-ble", "%s", msg);
}
*/
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"unsafe"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/experimental/deprecated"
	"github.com/sagernet/sing-box/include"
	sblog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/reticulum"
	"github.com/sagernet/sing/service"
)

func main() {}

// logcatLogger implements log.ContextLogger by writing to Android logcat.
type logcatLogger struct{}

func (logcatLogger) emit(prio C.int, args ...any) {
	s := fmt.Sprint(args...)
	cs := C.CString(s)
	C.do_log(prio, cs)
	C.free(unsafe.Pointer(cs))
}

func (l logcatLogger) Trace(a ...any)                           { l.emit(C.ANDROID_LOG_VERBOSE, a...) }
func (l logcatLogger) Debug(a ...any)                           { l.emit(C.ANDROID_LOG_DEBUG, a...) }
func (l logcatLogger) Info(a ...any)                            { l.emit(C.ANDROID_LOG_INFO, a...) }
func (l logcatLogger) Warn(a ...any)                            { l.emit(C.ANDROID_LOG_WARN, a...) }
func (l logcatLogger) Error(a ...any)                           { l.emit(C.ANDROID_LOG_ERROR, a...) }
func (l logcatLogger) Fatal(a ...any)                           { l.emit(C.ANDROID_LOG_FATAL, a...) }
func (l logcatLogger) Panic(a ...any)                           { l.emit(C.ANDROID_LOG_FATAL, a...) }
func (l logcatLogger) TraceContext(_ context.Context, a ...any) { l.Trace(a...) }
func (l logcatLogger) DebugContext(_ context.Context, a ...any) { l.Debug(a...) }
func (l logcatLogger) InfoContext(_ context.Context, a ...any)  { l.Info(a...) }
func (l logcatLogger) WarnContext(_ context.Context, a ...any)  { l.Warn(a...) }
func (l logcatLogger) ErrorContext(_ context.Context, a ...any) { l.Error(a...) }
func (l logcatLogger) FatalContext(_ context.Context, a ...any) { l.Fatal(a...) }
func (l logcatLogger) PanicContext(_ context.Context, a ...any) { l.Panic(a...) }

// compile-time check that logcatLogger satisfies the interface
var _ sblog.ContextLogger = logcatLogger{}

var (
	singBoxMu       sync.Mutex
	singBoxInstance *box.Box
	singBoxCancel   context.CancelFunc
)

//export Java_com_singbox_ble_Bridge_nativeSetJVM
func Java_com_singbox_ble_Bridge_nativeSetJVM(env *C.JNIEnv, cls C.jobject) {
	reticulum.BridgeSetJVM(unsafe.Pointer(C.extract_jvm(env)))
}

//export Java_com_singbox_ble_Bridge_nativeInit
func Java_com_singbox_ble_Bridge_nativeInit(env *C.JNIEnv, cls C.jobject, configJSON C.jstring, storagePathJ C.jstring) C.jint {
	cstr := C.jni_get_utf(env, configJSON)
	defer C.jni_release_utf(env, configJSON, cstr)
	raw := C.GoString((*C.char)(unsafe.Pointer(cstr)))

	spcs := C.jni_get_utf(env, storagePathJ)
	defer C.jni_release_utf(env, storagePathJ, spcs)
	storagePath := C.GoString((*C.char)(unsafe.Pointer(spcs)))

	// Override reticulum_config.storage_path with the app's private files dir.
	// /data/local/tmp is writable via adb but may be blocked by SELinux for app processes.
	raw = injectReticulumStoragePath(raw, storagePath)

	// Build context with all sing-box protocol registries. Required for:
	//   - option.Options.UnmarshalJSONContext (outbound type resolution)
	//   - box.New (inbound/outbound/endpoint managers)
	baseCtx := include.Context(
		service.ContextWith(context.Background(),
			deprecated.NewStderrManager(sblog.StdLogger())),
	)

	// Parse the full sing-box config (inbounds + outbounds + route).
	var opts option.Options
	if err := opts.UnmarshalJSONContext(baseCtx, []byte(raw)); err != nil {
		logToLogcat(C.ANDROID_LOG_ERROR, fmt.Sprintf("parse sing-box config: %v", err))
		return -1
	}

	ctx, cancel := context.WithCancel(baseCtx)

	// Set Rust log level to trace before box.Start() triggers BridgeInit.
	// The Reticulum outbound's Start() calls setRustLogLevelIfUnset, which is a
	// no-op when RUST_LOG is already set. Without this it defaults to "info"
	// because the sing-box logger doesn't expose a Level() method.
	if os.Getenv("RUST_LOG") == "" {
		os.Setenv("RUST_LOG", "trace,serde=off,jni=off")
	}

	// Start the full sing-box box. This drives:
	//   - mixed inbound → SOCKS5 proxy on :1080 (for e2e-loadtest)
	//   - reticulum outbound → calls BridgeSetLogger + BridgeInit internally
	// BridgeSetJVM was already called from nativeSetJVM (btleplug is ready).
	instance, err := box.New(box.Options{Context: ctx, Options: opts})
	if err != nil {
		cancel()
		logToLogcat(C.ANDROID_LOG_ERROR, fmt.Sprintf("box.New: %v", err))
		return -1
	}
	if err := instance.Start(); err != nil {
		cancel()
		instance.Close()
		logToLogcat(C.ANDROID_LOG_ERROR, fmt.Sprintf("box.Start: %v", err))
		return -1
	}

	// Restore logcatLogger as the Rust log sink. outbound.Start() called
	// BridgeSetLogger with the sing-box log factory logger, which on Android
	// writes to stdout → /dev/null (not logcat). Override it here so Rust logs
	// remain visible via adb logcat -s sing-box-ble:V.
	reticulum.BridgeSetLogger(logcatLogger{})

	singBoxMu.Lock()
	singBoxInstance = instance
	singBoxCancel = cancel
	singBoxMu.Unlock()

	logToLogcat(C.ANDROID_LOG_INFO, "bridge started (full sing-box box)")
	return 0
}

//export Java_com_singbox_ble_Bridge_nativeShutdown
func Java_com_singbox_ble_Bridge_nativeShutdown(env *C.JNIEnv, cls C.jobject) {
	singBoxMu.Lock()
	instance := singBoxInstance
	cancel := singBoxCancel
	singBoxInstance = nil
	singBoxCancel = nil
	singBoxMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if instance != nil {
		instance.Close()
		// Reticulum outbound.Close() calls BridgeShutdown() — no explicit call needed.
	}
}

func logToLogcat(prio C.int, msg string) {
	cs := C.CString(msg)
	C.do_log(prio, cs)
	C.free(unsafe.Pointer(cs))
}

// injectReticulumStoragePath overwrites storage_path in the first reticulum
// outbound's reticulum_config within a full sing-box config JSON.
func injectReticulumStoragePath(raw, storagePath string) string {
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &top); err != nil {
		return raw
	}
	outboundsRaw, ok := top["outbounds"]
	if !ok {
		return raw
	}
	var outbounds []map[string]json.RawMessage
	if err := json.Unmarshal(outboundsRaw, &outbounds); err != nil {
		return raw
	}
	modified := false
	for i, ob := range outbounds {
		typeRaw, ok := ob["type"]
		if !ok {
			continue
		}
		var obType string
		if err := json.Unmarshal(typeRaw, &obType); err != nil || obType != "reticulum" {
			continue
		}
		retCfgRaw, ok := ob["reticulum_config"]
		if !ok {
			continue
		}
		var retCfg map[string]json.RawMessage
		if err := json.Unmarshal(retCfgRaw, &retCfg); err != nil {
			continue
		}
		pathBytes, _ := json.Marshal(storagePath)
		retCfg["storage_path"] = pathBytes
		newRetCfgBytes, err := json.Marshal(retCfg)
		if err != nil {
			continue
		}
		outbounds[i]["reticulum_config"] = newRetCfgBytes
		modified = true
		break
	}
	if !modified {
		return raw
	}
	newOutboundsBytes, err := json.Marshal(outbounds)
	if err != nil {
		return raw
	}
	top["outbounds"] = newOutboundsBytes
	result, err := json.Marshal(top)
	if err != nil {
		return raw
	}
	return string(result)
}
