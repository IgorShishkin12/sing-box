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
	"unsafe"

	sblog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/protocol/reticulum"
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

func (l logcatLogger) Trace(a ...any)                               { l.emit(C.ANDROID_LOG_VERBOSE, a...) }
func (l logcatLogger) Debug(a ...any)                               { l.emit(C.ANDROID_LOG_DEBUG, a...) }
func (l logcatLogger) Info(a ...any)                                { l.emit(C.ANDROID_LOG_INFO, a...) }
func (l logcatLogger) Warn(a ...any)                                { l.emit(C.ANDROID_LOG_WARN, a...) }
func (l logcatLogger) Error(a ...any)                               { l.emit(C.ANDROID_LOG_ERROR, a...) }
func (l logcatLogger) Fatal(a ...any)                               { l.emit(C.ANDROID_LOG_FATAL, a...) }
func (l logcatLogger) Panic(a ...any)                               { l.emit(C.ANDROID_LOG_FATAL, a...) }
func (l logcatLogger) TraceContext(_ context.Context, a ...any)     { l.Trace(a...) }
func (l logcatLogger) DebugContext(_ context.Context, a ...any)     { l.Debug(a...) }
func (l logcatLogger) InfoContext(_ context.Context, a ...any)      { l.Info(a...) }
func (l logcatLogger) WarnContext(_ context.Context, a ...any)      { l.Warn(a...) }
func (l logcatLogger) ErrorContext(_ context.Context, a ...any)     { l.Error(a...) }
func (l logcatLogger) FatalContext(_ context.Context, a ...any)     { l.Fatal(a...) }
func (l logcatLogger) PanicContext(_ context.Context, a ...any)     { l.Panic(a...) }

// compile-time check that logcatLogger satisfies the interface
var _ sblog.ContextLogger = logcatLogger{}

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

	// BridgeInit expects only the reticulum_config sub-object with storage_path
	// pointing to the app's private files dir (not the shell-writable /data/local/tmp).
	cfg := injectStoragePath(extractReticulumConfig(raw), storagePath)

	reticulum.BridgeSetLogger(logcatLogger{})
	if err := reticulum.BridgeInit(cfg); err != nil {
		return -1
	}
	return 0
}

// injectStoragePath overwrites the storage_path field in a reticulum config JSON.
func injectStoragePath(cfg, path string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(cfg), &m); err != nil {
		return cfg
	}
	b, _ := json.Marshal(path)
	m["storage_path"] = b
	out, _ := json.Marshal(m)
	return string(out)
}

// extractReticulumConfig pulls the reticulum_config object from the first
// reticulum outbound in a full sing-box config JSON, or returns the input
// unchanged if it is already a bare reticulum config (no "outbounds" key).
func extractReticulumConfig(raw string) string {
	var top struct {
		Outbounds []struct {
			Type            string          `json:"type"`
			ReticulumConfig json.RawMessage `json:"reticulum_config"`
		} `json:"outbounds"`
	}
	if err := json.Unmarshal([]byte(raw), &top); err != nil || len(top.Outbounds) == 0 {
		return raw
	}
	for _, ob := range top.Outbounds {
		if ob.Type == "reticulum" && len(ob.ReticulumConfig) > 0 {
			return string(ob.ReticulumConfig)
		}
	}
	return raw
}

//export Java_com_singbox_ble_Bridge_nativeShutdown
func Java_com_singbox_ble_Bridge_nativeShutdown(env *C.JNIEnv, cls C.jobject) {
	reticulum.BridgeShutdown()
}
