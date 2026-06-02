#!/bin/bash
# real-device-test.sh — Run the E2E Reticulum test against a real Android phone.
#
# Prerequisites:
#   - adb is in PATH and exactly one real device is connected
#     (USB or: adb connect <phone-ip>:5555 before running this script)
#   - sing-box binary: looked up in order:
#       $SINGBOX_BIN env var → PATH → ../sing-box (make build_with_bridge output)
#   - sum-server binary: looked up in order:
#       $SUMSERVER_BIN env var → PATH → sumserver/sum-server (go build output)
#
# The script auto-discovers the PC's IP that is reachable from the phone.
# Override with: SERVER_IP=192.168.x.y ./real-device-test.sh
#
# Arm64 Android binaries are built on first run via Docker and cached in
# e2e/android-bins-arm64/. Subsequent runs reuse the cache.
#
# Future (Phase 2 APK): replace the "push binaries" section below with:
#   adb install path/to/sing-box-for-android.apk
#   adb push client-config.json \
#     /sdcard/Android/data/io.nekohasekai.sfa/files/config.json
#   adb shell am start -n io.nekohasekai.sfa/.bg.SFAService
# then wait for 127.0.0.1:1080 before running the loadtest.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINS_CACHE="$SCRIPT_DIR/android-bins-arm64"
SERVER_RETICULUM_PORT=7788
SINGBOX_STARTUP_WAIT=5

# ---------------------------------------------------------------------------
# 1. Prerequisite checks
# ---------------------------------------------------------------------------
echo "=== Checking prerequisites ==="

if ! command -v adb &>/dev/null; then
    echo "ERROR: adb not found in PATH" >&2; exit 1
fi

# Count real (non-emulator) devices
REAL_DEVICES=$(adb devices | tail -n +2 | grep -v '^$' | grep -v 'emulator' | grep 'device$' | wc -l)
if [[ "$REAL_DEVICES" -eq 0 ]]; then
    echo "ERROR: no real Android device connected." >&2
    echo "  USB: plug in phone and enable ADB debugging" >&2
    echo "  Network: adb connect <phone-ip>:5555" >&2
    exit 1
fi
if [[ "$REAL_DEVICES" -gt 1 ]]; then
    echo "ERROR: multiple real devices connected; disconnect all but one." >&2
    adb devices
    exit 1
fi

# Locate sing-box: PATH, then the parent directory (where `make build_with_bridge` drops it)
SINGBOX_BIN="${SINGBOX_BIN:-}"
if [[ -z "$SINGBOX_BIN" ]]; then
    if command -v sing-box &>/dev/null; then
        SINGBOX_BIN="$(command -v sing-box)"
    elif [[ -x "$SCRIPT_DIR/../sing-box" ]]; then
        SINGBOX_BIN="$(cd "$SCRIPT_DIR/.." && pwd)/sing-box"
    else
        echo "ERROR: sing-box not found. Build with 'make build_with_bridge' in sing-box/" >&2
        echo "  or set SINGBOX_BIN=/path/to/sing-box" >&2
        exit 1
    fi
fi
echo "sing-box: $SINGBOX_BIN"

# Locate sum-server: PATH, then the sumserver build directory
SUMSERVER_BIN="${SUMSERVER_BIN:-}"
if [[ -z "$SUMSERVER_BIN" ]]; then
    if command -v sum-server &>/dev/null; then
        SUMSERVER_BIN="$(command -v sum-server)"
    elif [[ -x "$SCRIPT_DIR/sumserver/sum-server" ]]; then
        SUMSERVER_BIN="$SCRIPT_DIR/sumserver/sum-server"
    else
        echo "ERROR: sum-server not found. Build with 'go build -o e2e/sumserver/sum-server ./e2e/sumserver/' in sing-box/" >&2
        echo "  or set SUMSERVER_BIN=/path/to/sum-server" >&2
        exit 1
    fi
fi
echo "sum-server: $SUMSERVER_BIN"

# ---------------------------------------------------------------------------
# 2. Auto-discover SERVER_IP
#    Get the phone's LAN IP → ask the PC's routing table which src IP it would
#    use to reach that address → that's the PC IP the phone can dial.
# ---------------------------------------------------------------------------
if [[ -z "${SERVER_IP:-}" ]]; then
    echo "=== Discovering network addresses ==="

    # Phone's LAN IP: look for inet address on a non-loopback wifi/eth interface
    PHONE_IP=$(adb shell ip addr \
        | grep 'inet ' \
        | grep -v '127\.' \
        | grep -v '::' \
        | awk '{print $2}' \
        | cut -d/ -f1 \
        | head -1 \
        | tr -d '\r')

    if [[ -z "$PHONE_IP" ]]; then
        echo "ERROR: could not determine phone's IP address." >&2
        echo "  Set SERVER_IP manually: SERVER_IP=x.x.x.x $0" >&2
        exit 1
    fi
    echo "Phone IP: $PHONE_IP"

    # PC's IP on the same subnet as the phone
    SERVER_IP=$(ip route get "$PHONE_IP" \
        | grep -oP 'src \K[\d.]+' \
        | head -1)

    if [[ -z "$SERVER_IP" ]]; then
        echo "ERROR: could not determine PC IP reachable from phone." >&2
        echo "  Set SERVER_IP manually: SERVER_IP=x.x.x.x $0" >&2
        exit 1
    fi
fi
echo "Server IP (PC): $SERVER_IP"

# ---------------------------------------------------------------------------
# 3. Build arm64 Android binaries if not already cached
# ---------------------------------------------------------------------------
if [[ ! -f "$BINS_CACHE/sing-box" || ! -f "$BINS_CACHE/e2e-loadtest" ]]; then
    echo "=== Building arm64 Android binaries via Docker (cached after first run) ==="
    mkdir -p "$BINS_CACHE"
    docker build \
        --build-arg GOARCH=arm64 \
        --build-arg RUST_TARGET=aarch64-linux-android \
        --build-arg NDK_CC=aarch64-linux-android34-clang \
        --target android-builder \
        --output "type=local,dest=$BINS_CACHE" \
        -f "$SCRIPT_DIR/Dockerfile.android-client" \
        "$(dirname "$SCRIPT_DIR")"
    echo "Binaries cached in $BINS_CACHE"
else
    echo "=== Reusing cached arm64 binaries from $BINS_CACHE ==="
fi

# ---------------------------------------------------------------------------
# 4. Start server on PC (background; cleaned up on EXIT)
# ---------------------------------------------------------------------------
echo "=== Starting PC-side server ==="
RETICULUM_STORAGE="$(mktemp -d)"
SERVER_CONFIG="$SCRIPT_DIR/configs/server.json"

"$SUMSERVER_BIN" &
SUMSERVER_PID=$!

RETICULUM_STORAGE="$RETICULUM_STORAGE" \
"$SINGBOX_BIN" run -c "$SERVER_CONFIG" &
SINGBOX_SERVER_PID=$!

cleanup() {
    echo "=== Cleanup ==="
    adb shell pkill -f 'sing-box' 2>/dev/null || true
    kill "$SINGBOX_SERVER_PID" "$SUMSERVER_PID" 2>/dev/null || true
    rm -rf "$RETICULUM_STORAGE"
}
trap cleanup EXIT

sleep 2  # let server initialise

# ---------------------------------------------------------------------------
# 5. Generate client config with discovered SERVER_IP
# ---------------------------------------------------------------------------
CLIENT_CONFIG=$(mktemp /tmp/sb-real-device-client-XXXXXX.json)
cat > "$CLIENT_CONFIG" <<EOF
{
  "log": {
    "level": "info",
    "output": "/dev/stdout",
    "timestamp": true
  },
  "inbounds": [
    {
      "type": "mixed",
      "tag": "mixed-in",
      "listen": "127.0.0.1",
      "listen_port": 1080
    }
  ],
  "outbounds": [
    {
      "type": "reticulum",
      "tag": "reticulum-out",
      "name": "e2e-sum-server",
      "password": "e2e-test-password",
      "reticulum_config": {
        "identity_name": "e2e-real-device-client",
        "storage_path": "/data/local/tmp/reticulum",
        "interfaces": [
          {
            "name": "Real Device TCP Client",
            "type": "TCPClientInterface",
            "target_host": "$SERVER_IP",
            "target_port": $SERVER_RETICULUM_PORT
          }
        ]
      }
    }
  ],
  "route": {
    "final": "reticulum-out"
  }
}
EOF

# ---------------------------------------------------------------------------
# 6. Push binaries and config to phone
# ---------------------------------------------------------------------------
echo "=== Pushing binaries and config to phone ==="
adb push "$BINS_CACHE/sing-box"          /data/local/tmp/sing-box
adb push "$BINS_CACHE/e2e-loadtest"      /data/local/tmp/e2e-loadtest
adb push "$CLIENT_CONFIG"                /data/local/tmp/sing-box-config.json
adb shell chmod +x /data/local/tmp/sing-box /data/local/tmp/e2e-loadtest
adb shell mkdir -p /data/local/tmp/reticulum
rm -f "$CLIENT_CONFIG"

# ---------------------------------------------------------------------------
# 7. Start sing-box on phone (background)
# ---------------------------------------------------------------------------
echo "=== Starting sing-box on phone ==="
adb shell \
    "nohup /data/local/tmp/sing-box run \
        -c /data/local/tmp/sing-box-config.json \
        >/data/local/tmp/singbox.log 2>&1 &"
echo "Waiting ${SINGBOX_STARTUP_WAIT}s for sing-box to initialise..."
sleep $SINGBOX_STARTUP_WAIT

# ---------------------------------------------------------------------------
# 8. Run loadtest on phone
# ---------------------------------------------------------------------------
echo "=== Running e2e-loadtest on phone ==="
LOADTEST_RESULT=0
adb shell \
    "/data/local/tmp/e2e-loadtest \
        --socks 127.0.0.1:1080 \
        --url http://127.0.0.1:8080" || LOADTEST_RESULT=$?

# ---------------------------------------------------------------------------
# 9. Collect logs
# ---------------------------------------------------------------------------
echo ""
echo "=== sing-box log from phone ==="
adb shell cat /data/local/tmp/singbox.log 2>/dev/null || true

# ---------------------------------------------------------------------------
# 10. Report
# ---------------------------------------------------------------------------
echo ""
if [[ $LOADTEST_RESULT -eq 0 ]]; then
    echo "ALL REAL-DEVICE E2E TESTS PASSED"
else
    echo "REAL-DEVICE E2E TESTS FAILED (exit $LOADTEST_RESULT)"
fi
exit $LOADTEST_RESULT
