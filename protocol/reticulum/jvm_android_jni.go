//go:build android && with_reticulum

package reticulum

/*
#include <jni.h>

// extract_jvm pulls the process-wide JavaVM* out of a JNIEnv*. Declared static
// so it has internal linkage: cgo copies a file's preamble into more than one
// generated C output when that file contains //export directives, and a
// non-static definition would then collide at link time.
static void* extract_jvm(JNIEnv* env) {
    JavaVM* jvm = NULL;
    (*env)->GetJavaVM(env, &jvm);
    return (void*)jvm;
}
*/
import "C"

import "unsafe"

// Java_io_nekohasekai_sfa_ReticulumBle_nativeSetReticulumJVM is the JNI entry
// point invoked from the Android app's `io.nekohasekai.sfa.ReticulumBle` class.
//
// The Rust bridge is linked as a staticlib inside gomobile's libgojni.so, so it
// cannot capture the JavaVM via its own JNI_OnLoad (gomobile owns that). Instead
// Kotlin calls this on the app's main (Java) thread — whose JNIEnv carries the
// app classloader — letting btleplug/jni-utils resolve the bundled BLE support
// classes. Must run before the Reticulum outbound's Start() calls BridgeInit.
//
// The symbol is exported by the c-shared link even though this package is only a
// dependency of the gomobile-bound ./experimental/libbox package.
//
//export Java_io_nekohasekai_sfa_ReticulumBle_nativeSetReticulumJVM
func Java_io_nekohasekai_sfa_ReticulumBle_nativeSetReticulumJVM(env *C.JNIEnv, cls C.jobject) {
	BridgeSetJVM(unsafe.Pointer(C.extract_jvm(env)))
}
