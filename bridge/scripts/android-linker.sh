#!/usr/bin/env bash
#
# android-linker.sh — Wrapper script for the Android NDK linker.
#
# This script is used by .cargo/config.toml as the linker for
# aarch64-linux-android targets. It reads the ANDROID_NDK_HOME
# environment variable (or ANDROID_NDK as fallback) and delegates
# to the appropriate NDK clang binary.
#
# Usage:
#   ANDROID_NDK_HOME=/path/to/ndk ./android-linker.sh <cargo-linker-args...>
#
# Required:
#   ANDROID_NDK_HOME  — path to the Android NDK installation
#

set -euo pipefail

# --- Resolve NDK root -------------------------------------------------------
NDK_ROOT="${ANDROID_NDK_HOME:-${ANDROID_NDK:-}}"

if [[ -z "$NDK_ROOT" ]]; then
    echo "ERROR: android-linker.sh requires ANDROID_NDK_HOME or ANDROID_NDK to be set." >&2
    echo "       Example: export ANDROID_NDK_HOME=\$HOME/Android/Sdk/ndk/28.0.13004108" >&2
    exit 1
fi

if [[ ! -d "$NDK_ROOT" ]]; then
    echo "ERROR: ANDROID_NDK_HOME/ANDROID_NDK points to a non-existent directory:" >&2
    echo "       $NDK_ROOT" >&2
    exit 1
fi

# --- Locate the NDK clang ---------------------------------------------------
# NDK r28+ places the toolchain under:
#   $NDK_ROOT/toolchains/llvm/prebuilt/linux-x86_64/bin/
# The aarch64-linux-android21-clang (or higher API level) is the correct linker.
HOST_TAG="linux-x86_64"
TOOLCHAIN_DIR="$NDK_ROOT/toolchains/llvm/prebuilt/$HOST_TAG"
CLANG="$TOOLCHAIN_DIR/bin/aarch64-linux-android21-clang"

if [[ ! -x "$CLANG" ]]; then
    # Fallback: try to find any aarch64-linux-android*-clang
    CLANG=$(find "$TOOLCHAIN_DIR/bin" -maxdepth 1 -name 'aarch64-linux-android*-clang' -type f 2>/dev/null | head -1 || true)
fi

if [[ -z "$CLANG" || ! -x "$CLANG" ]]; then
    echo "ERROR: Could not find aarch64-linux-android clang in NDK toolchain." >&2
    echo "       Searched: $TOOLCHAIN_DIR/bin/" >&2
    echo "       Ensure the NDK is complete and the host tag '$HOST_TAG' is correct." >&2
    exit 1
fi

# --- Delegate to the NDK clang ----------------------------------------------
exec "$CLANG" "$@"
