#!/bin/bash
# Android E2E test entrypoint.
#
# Runs inside the e2e-android-client container. Boots the Android emulator,
# bridges the Docker network into it via socat, pushes the pre-built sing-box
# and loadtest binaries via ADB, runs the full 3-phase load test, and exits
# with the loadtest exit code.
#
# Phase 2 (APK) upgrade path — when the Android app is built, replace the
# binary push block (section "push binaries") with:
#   adb -e install /android-bins/reticulum.apk
#   adb -e shell am start -n com.example.reticulum/.MainActivity
# and wait for the SOCKS5 port to become ready before running the loadtest.
set -euo pipefail

ADB="adb -e"
SOCAT_PORT=7789        # container-side listener; emulator reaches it via 10.0.2.2
SERVER_RETICULUM_PORT=7788
BOOT_TIMEOUT=300       # seconds to wait for emulator boot
SINGBOX_STARTUP_WAIT=5 # seconds for sing-box to initialise inside emulator

# ---------------------------------------------------------------------------
# 1. Resolve server hostname to an IP the container can reach
# ---------------------------------------------------------------------------
echo "=== Resolving e2e-server ==="
SERVER_IP=$(getent hosts e2e-server | awk '{print $1; exit}')
if [[ -z "$SERVER_IP" ]]; then
    echo "ERROR: could not resolve e2e-server hostname" >&2
    exit 1
fi
echo "e2e-server -> $SERVER_IP"

# ---------------------------------------------------------------------------
# 2. socat bridge: container port -> Docker network -> e2e-server Reticulum port
#    Inside the emulator, 10.0.2.2 is the container host, so sing-box's
#    TCPClientInterface connects to 10.0.2.2:$SOCAT_PORT which this bridge
#    forwards to $SERVER_IP:$SERVER_RETICULUM_PORT.
# ---------------------------------------------------------------------------
echo "=== Starting socat bridge (0.0.0.0:$SOCAT_PORT -> $SERVER_IP:$SERVER_RETICULUM_PORT) ==="
socat TCP-LISTEN:$SOCAT_PORT,fork,reuseaddr \
    TCP:$SERVER_IP:$SERVER_RETICULUM_PORT &
SOCAT_PID=$!
trap 'kill $SOCAT_PID 2>/dev/null || true' EXIT

# ---------------------------------------------------------------------------
# 3. Start Android emulator (headless, KVM-accelerated, no snapshot)
# ---------------------------------------------------------------------------
echo "=== Starting Android emulator ==="
$ANDROID_HOME/emulator/emulator \
    -avd reticulum_test \
    -no-window \
    -no-audio \
    -gpu swiftshader_indirect \
    -no-snapshot \
    -no-boot-anim \
    &>/tmp/emulator.log &
EMULATOR_PID=$!
trap 'kill $EMULATOR_PID $SOCAT_PID 2>/dev/null || true' EXIT

# ---------------------------------------------------------------------------
# 4. Wait for the emulator to finish booting
# ---------------------------------------------------------------------------
echo "=== Waiting for emulator boot (up to ${BOOT_TIMEOUT}s) ==="
START=$(date +%s)
while true; do
    BOOT=$(adb -e shell getprop sys.boot_completed 2>/dev/null | tr -d '\r' || true)
    if [[ "$BOOT" == "1" ]]; then
        echo "Emulator booted in $(( $(date +%s) - START ))s"
        break
    fi
    if (( $(date +%s) - START >= BOOT_TIMEOUT )); then
        echo "ERROR: emulator did not boot within ${BOOT_TIMEOUT}s" >&2
        echo "--- emulator log ---"
        cat /tmp/emulator.log || true
        exit 1
    fi
    echo "  still booting... ($(( $(date +%s) - START ))s elapsed)"
    sleep 5
done

# ---------------------------------------------------------------------------
# 5. Generate sing-box config targeting the socat bridge via 10.0.2.2
# ---------------------------------------------------------------------------
cat > /tmp/android-config.json <<EOF
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
        "identity_name": "e2e-android-client",
        "storage_path": "/data/local/tmp/reticulum",
        "interfaces": [
          {
            "name": "Android TCP Client",
            "type": "TCPClientInterface",
            "target_host": "10.0.2.2",
            "target_port": $SOCAT_PORT
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
# 6. Push binaries and config into the emulator
# ---------------------------------------------------------------------------
echo "=== Pushing binaries and config to emulator ==="
$ADB shell mkdir -p /data/local/tmp/reticulum
$ADB push /android-bins/sing-box          /data/local/tmp/sing-box
$ADB push /android-bins/e2e-loadtest      /data/local/tmp/e2e-loadtest
$ADB push /tmp/android-config.json        /data/local/tmp/sing-box-config.json
$ADB shell chmod +x /data/local/tmp/sing-box /data/local/tmp/e2e-loadtest

# ---------------------------------------------------------------------------
# 7. Start sing-box inside the emulator (background)
# ---------------------------------------------------------------------------
echo "=== Starting sing-box on Android emulator ==="
$ADB shell \
    "nohup /data/local/tmp/sing-box run \
        -c /data/local/tmp/sing-box-config.json \
        >/data/local/tmp/singbox.log 2>&1 &"
echo "Waiting ${SINGBOX_STARTUP_WAIT}s for sing-box to initialise..."
sleep $SINGBOX_STARTUP_WAIT

# ---------------------------------------------------------------------------
# 8. Run the load test inside the emulator
# ---------------------------------------------------------------------------
echo "=== Running e2e-loadtest on Android emulator ==="
LOADTEST_RESULT=0
$ADB shell \
    "/data/local/tmp/e2e-loadtest \
        --socks 127.0.0.1:1080 \
        --url http://127.0.0.1:8080" || LOADTEST_RESULT=$?

# ---------------------------------------------------------------------------
# 9. Collect logs from emulator
# ---------------------------------------------------------------------------
echo ""
echo "=== sing-box log from Android emulator ==="
$ADB shell cat /data/local/tmp/singbox.log 2>/dev/null || true

# ---------------------------------------------------------------------------
# 10. Report result
# ---------------------------------------------------------------------------
echo ""
if [[ $LOADTEST_RESULT -eq 0 ]]; then
    echo "ALL ANDROID E2E TESTS PASSED"
else
    echo "ANDROID E2E TESTS FAILED (exit $LOADTEST_RESULT)"
fi
exit $LOADTEST_RESULT
