//go:build android && with_reticulum

// Package main compiles as a JNI shared library (-buildmode c-shared) for Android.
// It exposes three JNI methods that a minimal Android Service calls to run the
// Reticulum bridge with BLE support.
//
// Build:
//
//	GOOS=android GOARCH=arm64 CGO_ENABLED=1 CC=<ndk-clang> \
//	  go build -buildmode c-shared -tags "with_reticulum" \
//	  -o libsing-box-ble.so ./cmd/ble-launcher/
package main

/*
#include <jni.h>
#include <stdlib.h>
#include "../../bridge/include/reticulum_bridge.h"

// Extract JavaVM* from a live JNIEnv*. Used to pass to reticulum_set_jvm on
// the calling Java thread so btleplug's Adapter::new() has a bound JNIEnv.
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
*/
import "C"
import (
	"unsafe"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/protocol/reticulum"
)

func main() {}

// Java_com_singbox_ble_Bridge_nativeSetJVM passes the JavaVM to the Rust
// bridge and triggers btleplug platform initialisation on this Java thread.
// Must be called from Service.onCreate() before nativeInit.
//
//export Java_com_singbox_ble_Bridge_nativeSetJVM
func Java_com_singbox_ble_Bridge_nativeSetJVM(env *C.JNIEnv, cls C.jobject) {
	jvm := C.extract_jvm(env)
	reticulum.BridgeSetJVM(unsafe.Pointer(jvm))
}

// Java_com_singbox_ble_Bridge_nativeInit initialises the bridge with the
// given JSON config. Returns 0 on success, -1 on error.
//
//export Java_com_singbox_ble_Bridge_nativeInit
func Java_com_singbox_ble_Bridge_nativeInit(env *C.JNIEnv, cls C.jobject, configJSON C.jstring) C.jint {
	cstr := C.jni_get_utf(env, configJSON)
	defer C.jni_release_utf(env, configJSON, cstr)
	cfg := C.GoString((*C.char)(unsafe.Pointer(cstr)))

	reticulum.BridgeSetLogger(log.NewNOPFactory().Logger())
	if err := reticulum.BridgeInit(cfg); err != nil {
		return -1
	}
	return 0
}

// Java_com_singbox_ble_Bridge_nativeShutdown tears down the bridge.
//
//export Java_com_singbox_ble_Bridge_nativeShutdown
func Java_com_singbox_ble_Bridge_nativeShutdown(env *C.JNIEnv, cls C.jobject) {
	reticulum.BridgeShutdown()
}
