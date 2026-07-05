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
# Arm64 Android binaries are built on first run via Podman (or Docker) and cached in
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
BINS_CACHE="$SCRIPT_DIR/android-bins"
SERVER_RETICULUM_PORT=7788
SINGBOX_STARTUP_WAIT=5
LOG_DIR="$SCRIPT_DIR/logs/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG_DIR"
echo "Logs → $LOG_DIR"

# Prefer podman; fall back to docker
if command -v podman &>/dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &>/dev/null; then
    CONTAINER_CMD="docker"
else
    CONTAINER_CMD=""
fi

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

# Check that required ports are free
for PORT in 8080 7788; do
    if ss -tlnp 2>/dev/null | grep -q ":$PORT " || \
       netstat -tlnp 2>/dev/null | grep -q ":$PORT "; then
        echo "ERROR: port $PORT is already in use (leftover from a previous run?)." >&2
        echo "  Find and kill the process: lsof -i :$PORT" >&2
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# 2. Auto-discover SERVER_IP
#    Get the phone's LAN IP → ask the PC's routing table which src IP it would
#    use to reach that address → that's the PC IP the phone can dial.
# ---------------------------------------------------------------------------
if [[ -z "${SERVER_IP:-}" ]]; then
    echo "=== Discovering network addresses ==="

    # Phone's WiFi IP — prefer wlan0, fall back to any non-loopback non-link-local address
    PHONE_IP=$(adb shell "ip addr show wlan0 2>/dev/null | grep 'inet '" \
        | awk '{print $2}' | cut -d/ -f1 | tr -d '\r') || true
    if [[ -z "$PHONE_IP" ]]; then
        PHONE_IP=$(adb shell ip addr \
            | grep 'inet ' \
            | grep -v '127\.' \
            | grep -v '169\.254\.' \
            | grep -v '10\.' \
            | awk '{print $2}' \
            | cut -d/ -f1 \
            | head -1 \
            | tr -d '\r') || true
    fi

    if [[ -z "$PHONE_IP" ]]; then
        echo "ERROR: could not determine phone's IP address." >&2
        echo "  Set SERVER_IP manually: SERVER_IP=x.x.x.x $0" >&2
        exit 1
    fi
    echo "Phone IP: $PHONE_IP"

    # PC's LAN IP: the source address the kernel would use to reach the phone,
    # but excluding Docker/bridge virtual interfaces (172.x or dev docker*/br-*).
    SERVER_IP=$(ip route get "$PHONE_IP" \
        | grep -v ' dev \(docker\|br-\)' \
        | grep -oP 'src \K[\d.]+' \
        | grep -v '^172\.' \
        | head -1) || true

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
    if [[ -z "$CONTAINER_CMD" ]]; then
        echo "ERROR: arm64 binaries not found and neither podman nor docker is available." >&2
        echo "  Pre-build them or install podman/docker." >&2
        exit 1
    fi
    echo "=== Building arm64 Android binaries via $CONTAINER_CMD (cached after first run) ==="
    mkdir -p "$BINS_CACHE"
    "$CONTAINER_CMD" build \
        --build-arg GOARCH=arm64 \
        --build-arg RUST_TARGET=aarch64-linux-android \
        --build-arg NDK_CC=aarch64-linux-android34-clang \
        --target export \
        --output "type=local,dest=$SCRIPT_DIR" \
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

# Patch storage_path in the server config to a writable temp dir
SERVER_CONFIG="$(mktemp /tmp/sb-real-device-server-XXXXXX.json)"
sed "s|\"storage_path\":.*|\"storage_path\": \"$RETICULUM_STORAGE\",|" \
    "$SCRIPT_DIR/configs/server.json" > "$SERVER_CONFIG"

# Process substitution keeps $! as the binary's PID, not tee's.
ADDR=0.0.0.0 "$SUMSERVER_BIN" > >(tee "$LOG_DIR/pc-sumserver.log") 2>&1 &
SUMSERVER_PID=$!

"$SINGBOX_BIN" run -c "$SERVER_CONFIG" > >(tee "$LOG_DIR/pc-singbox.log") 2>&1 &
SINGBOX_SERVER_PID=$!
SINGBOX_ANDROID_PID=""  # set later; initialised here so cleanup is always safe

cleanup() {
    echo "=== Cleanup ==="
    adb shell pkill -f '/data/local/tmp/sing-box' 2>/dev/null || true
    kill "$SINGBOX_SERVER_PID" ${SINGBOX_ANDROID_PID:+"$SINGBOX_ANDROID_PID"} "$SUMSERVER_PID" 2>/dev/null || true
    rm -f "$SERVER_CONFIG"
    rm -rf "$RETICULUM_STORAGE"
    echo "Logs saved in $LOG_DIR"
}
trap cleanup EXIT

sleep 2  # let server initialise

# ---------------------------------------------------------------------------
# Pre-check: verify phone can reach sum-server directly (no sing-box)
# sum-server is bound on 0.0.0.0:8080 so the phone can POST to it directly.
# ---------------------------------------------------------------------------
echo "=== Pre-check: phone → sum-server ($SERVER_IP:8080) without sing-box ==="
PRECHECK_RESPONSE=$(adb shell \
    "printf 'POST /sum HTTP/1.0\r\nHost: $SERVER_IP:8080\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"a\":3,\"b\":5}\r\n' \
     | nc -w 5 $SERVER_IP 8080 2>/dev/null" \
    | tr -d '\r') || true

if echo "$PRECHECK_RESPONSE" | grep -q '"sum":8'; then
    echo "Pre-check OK: phone can reach sum-server at $SERVER_IP:8080"
else
    echo "ERROR: pre-check failed — phone cannot reach sum-server at $SERVER_IP:8080" >&2
    echo "  Response: ${PRECHECK_RESPONSE:-(empty)}" >&2
    echo "  Check:" >&2
    echo "    - SERVER_IP=$SERVER_IP is reachable from the phone" >&2
    echo "    - No firewall blocks port 8080 on the PC" >&2
    echo "    - Phone and PC are on the same network" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 5. Generate client config with discovered SERVER_IP
# ---------------------------------------------------------------------------
CLIENT_CONFIG=$(mktemp /tmp/sb-real-device-client-XXXXXX.json)
cat > "$CLIENT_CONFIG" <<EOF
{
  "log": {
    "level": "trace",
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
      "auth_retry": "exp",
      "auth_on_start": true,
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
# Wipe stale Reticulum state — the server gets a fresh identity every run
# (new temp dir = new key = new destination hash), so any cached
# name→hash mapping on the phone would point to the wrong destination.
adb shell rm -rf /data/local/tmp/reticulum
adb shell mkdir -p /data/local/tmp/reticulum
rm -f "$CLIENT_CONFIG"

# ---------------------------------------------------------------------------
# 7. Start sing-box on phone — stream output to PC in real-time
# ---------------------------------------------------------------------------
echo "=== Starting sing-box on phone ==="
# Run sing-box in the foreground inside adb shell; the background adb process
# on the PC side pipes stdout/stderr here and into the log file live.
adb shell "/data/local/tmp/sing-box run -c /data/local/tmp/sing-box-config.json" \
    2>&1 | tee "$LOG_DIR/android-singbox.log" &
SINGBOX_ANDROID_PID=$!

echo "Waiting ${SINGBOX_STARTUP_WAIT}s for sing-box to initialise..."
sleep $SINGBOX_STARTUP_WAIT

# ---------------------------------------------------------------------------
# 8. Run loadtest on phone
# ---------------------------------------------------------------------------
echo "=== Running e2e-loadtest on phone ==="
LOADTEST_RESULT=0
adb shell "/data/local/tmp/e2e-loadtest --socks 127.0.0.1:1080 --url http://127.0.0.1:8080" \
    2>&1 | tee "$LOG_DIR/android-loadtest.log" || LOADTEST_RESULT=$?

# ---------------------------------------------------------------------------
# 9. Report
# ---------------------------------------------------------------------------
echo ""
if [[ $LOADTEST_RESULT -eq 0 ]]; then
    echo "ALL REAL-DEVICE E2E TESTS PASSED"
else
    echo "REAL-DEVICE E2E TESTS FAILED (exit $LOADTEST_RESULT)"
fi
exit $LOADTEST_RESULT
