#!/usr/bin/env bash
#
# build-android.sh — Cross-compile the Rust bridge crate for Android (aarch64).
#
# Prerequisites:
#   - Rust toolchain with the `aarch64-linux-android` target installed.
#     Install with: rustup target add aarch64-linux-android
#   - Android NDK installed and ANDROID_NDK_HOME (or ANDROID_NDK) set.
#     Example: export ANDROID_NDK_HOME=$HOME/Android/Sdk/ndk/28.0.13004108
#
# Usage:
#   ./scripts/build-android.sh [--release]
#
# Options:
#   --release    Build in release mode (default: debug)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRIDGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE="${1:-debug}"

if [[ "$PROFILE" == "--release" ]]; then
    PROFILE="release"
    CARGO_FLAGS="--release"
else
    PROFILE="debug"
    CARGO_FLAGS=""
fi

echo "============================================"
echo "  Rust Bridge — Android Cross-Compile"
echo "============================================"
echo "  Bridge dir : $BRIDGE_DIR"
echo "  Profile    : $PROFILE"
echo "  Target     : aarch64-linux-android"
echo ""

# --- Check prerequisites ----------------------------------------------------

# 1. ANDROID_NDK_HOME / ANDROID_NDK
NDK_ROOT="${ANDROID_NDK_HOME:-${ANDROID_NDK:-}}"
if [[ -z "$NDK_ROOT" ]]; then
    echo "ERROR: ANDROID_NDK_HOME or ANDROID_NDK must be set." >&2
    echo "       Example: export ANDROID_NDK_HOME=\$HOME/Android/Sdk/ndk/28.0.13004108" >&2
    exit 1
fi
if [[ ! -d "$NDK_ROOT" ]]; then
    echo "ERROR: ANDROID_NDK_HOME/ANDROID_NDK points to a non-existent directory:" >&2
    echo "       $NDK_ROOT" >&2
    exit 1
fi
echo "  NDK root   : $NDK_ROOT"

# 2. Rust target
if ! rustup target list --installed 2>/dev/null | grep -q '^aarch64-linux-android$'; then
    echo "ERROR: Rust target 'aarch64-linux-android' is not installed." >&2
    echo "       Install it with: rustup target add aarch64-linux-android" >&2
    exit 1
fi
echo "  Rust target: aarch64-linux-android (installed)"

# 3. Linker wrapper
LINKER_WRAPPER="$BRIDGE_DIR/scripts/android-linker.sh"
if [[ ! -x "$LINKER_WRAPPER" ]]; then
    echo "ERROR: Linker wrapper not found or not executable:" >&2
    echo "       $LINKER_WRAPPER" >&2
    echo "       Run: chmod +x $LINKER_WRAPPER" >&2
    exit 1
fi

echo ""

# --- Build ------------------------------------------------------------------
echo ">>> Running cargo build ..."
cd "$BRIDGE_DIR"

# Ensure the linker wrapper is in PATH so .cargo/config.toml can find it
export PATH="$BRIDGE_DIR/scripts:$PATH"

# Set RUSTFLAGS for Android: position-independent code, static libc++ linking
export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-fPIC"

if cargo build --target aarch64-linux-android $CARGO_FLAGS; then
    echo ""
    echo "============================================"
    echo "  ✅ BUILD SUCCESS"
    echo "============================================"
    echo "  Artifacts:"
    echo "    $BRIDGE_DIR/target/aarch64-linux-android/$PROFILE/"
    echo "    $(find "$BRIDGE_DIR/target/aarch64-linux-android/$PROFILE" -maxdepth 1 -name '*.a' -o -name '*.so' 2>/dev/null | tr '\n' ' ')"
    echo ""
else
    echo ""
    echo "============================================"
    echo "  ❌ BUILD FAILED"
    echo "============================================"
    echo "  Check the error output above for details."
    echo "  Common issues:"
    echo "    - Missing NDK components"
    echo "    - Incompatible NDK version"
    echo "    - Missing Rust target"
    echo ""
    exit 1
fi
