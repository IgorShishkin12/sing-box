#!/usr/bin/env bash
# Hardware E2E test runner for RNodeSerial and RNodeBLE interfaces.
# Requires two physical RNode devices connected to the host.
# Not run in CI — invoke manually with the appropriate env vars set.
#
# Usage:
#   ./runner-hw.sh serial   # RNodeSerial over USB (both ends in containers)
#   ./runner-hw.sh ble      # RNodeBLE over Bluetooth (both ends in containers)
#   ./runner-hw.sh mixed    # container/serial server ↔ LoRa ↔ native/BLE client
#
# Environment variables (serial):
#   RNODE_SERIAL_SERVER  Host device path for server-side RNode (default: /dev/ttyUSB0)
#   RNODE_SERIAL_CLIENT  Host device path for client-side RNode (default: /dev/ttyUSB1)
#
# Environment variables (BLE):
#   RNODE_BLE_SERVER     BLE peripheral ID (name or MAC) for server-side RNode
#   RNODE_BLE_CLIENT     BLE peripheral ID (name or MAC) for client-side RNode
#
# Environment variables (mixed):
#   RNODE_SERIAL_SERVER  Host device path for the server-side (serial) RNode A
#   RNODE_BLE_CLIENT     BLE peripheral ID (name or MAC) for the client-side RNode B
#   SINGBOX_BIN          Optional: native sing-box binary (else built via make)
#   LOADTEST_BIN         Optional: native e2e-loadtest binary (else built via go)
#
# The "mixed" mode exercises the full path
#   container → serial → RNode A → LoRa → RNode B → BLE → native host.
# The serial end runs in a container (docker-compose.mixed.yml); the BLE end runs
# NATIVELY on the host (BlueZ), since BLE-from-container is a pain. The two ends
# talk over LoRa RF only, so no shared network is required.
#
# LoRa parameters MUST be identical on both ends (frequency_hz, bandwidth_hz,
# spreading_factor, coding_rate) or the link silently fails to form. The default
# configs pair server-serial.json ↔ client-ble.json at 433 MHz / 500 kHz / SF8 /
# CR6. Edit the configs to change; keep both sides in lockstep.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"
if command -v podman-compose &>/dev/null; then
    COMPOSE_CMD="podman-compose"
elif podman compose version &>/dev/null 2>&1; then
    PODMAN_SOCK="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"
    if [ -S "$PODMAN_SOCK" ]; then
        COMPOSE_CMD="podman compose"
    fi
fi
echo "Using compose: $COMPOSE_CMD"

run_test() {
    local file="$1"
    rc=0
    echo ""
    echo "=== Running: $file ==="
    $COMPOSE_CMD -f "$file" up \
        --exit-code-from e2e-client \
        --abort-on-container-exit || rc=1
    $COMPOSE_CMD -f "$file" down
    if [ $rc -ne 0 ]; then
        echo "HW E2E test failed: $file"
        exit 1
    fi
}

# Preflight: verify the host can actually drive a BLE adapter via BlueZ before we
# start a client that would otherwise fail deep inside btleplug with an opaque error.
# Escalates from "is BlueZ installed" → "is a controller present and powered" →
# "can we actually run an LE scan". Every failure prints the concrete fix command.
check_bluetooth() {
    echo "=== Preflight: Bluetooth (BlueZ) ==="

    # 1. bluetoothctl present ⇒ BlueZ userspace installed.
    if ! command -v bluetoothctl >/dev/null 2>&1; then
        echo "ERROR: bluetoothctl not found — BlueZ is not installed." >&2
        echo "  Fix: sudo apt install bluez && sudo systemctl enable --now bluetooth" >&2
        return 1
    fi

    # 2. bluetoothd reachable on the system D-Bus (org.bluez).
    if command -v busctl >/dev/null 2>&1 && ! busctl --system status org.bluez >/dev/null 2>&1; then
        echo "ERROR: BlueZ service (org.bluez) is not reachable on the system D-Bus." >&2
        echo "  Fix: sudo systemctl enable --now bluetooth" >&2
        return 1
    fi

    # 3. Adapter not rfkill-blocked.
    if command -v rfkill >/dev/null 2>&1 && rfkill list bluetooth 2>/dev/null | grep -qi 'blocked: yes'; then
        echo "ERROR: Bluetooth is rfkill-blocked." >&2
        echo "  Fix: sudo rfkill unblock bluetooth" >&2
        return 1
    fi

    # 4. At least one controller present.
    if ! bluetoothctl list 2>/dev/null | grep -q .; then
        echo "ERROR: no Bluetooth controller detected (bluetoothctl list is empty)." >&2
        echo "  Check the adapter is plugged in and its driver is loaded (dmesg | grep -i bluetooth)." >&2
        return 1
    fi

    # 5. Controller powered — try to power it on if not.
    if ! bluetoothctl show 2>/dev/null | grep -q 'Powered: yes'; then
        echo "Adapter not powered; attempting 'bluetoothctl power on'..."
        bluetoothctl power on >/dev/null 2>&1 || true
        if ! bluetoothctl show 2>/dev/null | grep -q 'Powered: yes'; then
            echo "ERROR: could not power on the Bluetooth adapter." >&2
            echo "  Fix: bluetoothctl power on   (may require sudo / an active login session)" >&2
            return 1
        fi
    fi

    # 6. Actually exercise an LE scan — the closest proxy for "btleplug will work".
    #    Guarded on --timeout support (bluez ≥ 5.55); skipped gracefully otherwise.
    #    Capture --help first: bluetoothctl exits non-zero for it, which would poison
    #    the pipeline status under `set -o pipefail` and skip the probe.
    local help_out
    help_out="$(bluetoothctl --help 2>&1 || true)"
    if printf '%s\n' "$help_out" | grep -q -- '--timeout'; then
        echo "Verifying an LE scan works (≈4s)..."
        local scan_out
        scan_out="$(bluetoothctl --timeout 4 scan on 2>&1)" || true
        if echo "$scan_out" | grep -qiE 'not available|no default controller|org\.freedesktop\.DBus\.Error|Failed to (start|set) discovery|Access denied'; then
            echo "ERROR: BLE scan failed — btleplug will not work either. Details:" >&2
            echo "$scan_out" | tail -5 | sed 's/^/    /' >&2
            echo "  Likely a D-Bus/polkit permission issue. If running over SSH/headless," >&2
            echo "  add your user to the 'bluetooth' group, use an active login session," >&2
            echo "  or run with sudo." >&2
            return 1
        fi
        echo "LE scan OK."
    else
        echo "(bluetoothctl lacks --timeout; skipping active scan probe)"
    fi

    echo "Bluetooth preflight passed."
}

MODE="${1:-serial}"

case "$MODE" in
  serial)
    : "${RNODE_SERIAL_SERVER:?Set RNODE_SERIAL_SERVER to server-side RNode device path (e.g. /dev/ttyUSB0)}"
    : "${RNODE_SERIAL_CLIENT:?Set RNODE_SERIAL_CLIENT to client-side RNode device path (e.g. /dev/ttyUSB1)}"
    export RNODE_SERIAL_SERVER RNODE_SERIAL_CLIENT

    echo "Building e2e images..."
    $COMPOSE_CMD -f docker-compose.serial.yml build

    run_test docker-compose.serial.yml
    ;;

  ble)
    : "${RNODE_BLE_SERVER:?Set RNODE_BLE_SERVER to server-side RNode BLE peripheral ID (name or MAC)}"
    : "${RNODE_BLE_CLIENT:?Set RNODE_BLE_CLIENT to client-side RNode BLE peripheral ID (name or MAC)}"
    export RNODE_BLE_SERVER RNODE_BLE_CLIENT

    check_bluetooth || exit 1

    # Inject peripheral IDs into config templates, producing resolved copies
    # that docker-compose.ble.yml mounts into the containers.
    envsubst '$RNODE_BLE_SERVER' < configs/server-ble.json > configs/server-ble-resolved.json
    envsubst '$RNODE_BLE_CLIENT' < configs/client-ble.json > configs/client-ble-resolved.json

    echo "Building e2e images..."
    $COMPOSE_CMD -f docker-compose.ble.yml build

    run_test docker-compose.ble.yml

    rm -f configs/server-ble-resolved.json configs/client-ble-resolved.json
    ;;

  mixed)
    # Full path: container(serial server) ↔ LoRa ↔ native(BLE client).
    : "${RNODE_SERIAL_SERVER:?Set RNODE_SERIAL_SERVER to the server-side (serial) RNode device path (e.g. /dev/ttyUSB0)}"
    : "${RNODE_BLE_CLIENT:?Set RNODE_BLE_CLIENT to the client-side RNode BLE peripheral ID (name or MAC)}"
    export RNODE_SERIAL_SERVER RNODE_BLE_CLIENT

    check_bluetooth || exit 1

    SB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    LOG_DIR="$SCRIPT_DIR/logs/mixed-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$LOG_DIR"
    echo "Logs → $LOG_DIR"

    # 1. Locate or build the native host sing-box (Rust bridge + rnode-ble via BlueZ).
    SINGBOX_BIN="${SINGBOX_BIN:-}"
    if [ -z "$SINGBOX_BIN" ]; then
        if [ -x "$SB_ROOT/sing-box" ]; then
            SINGBOX_BIN="$SB_ROOT/sing-box"
        elif command -v sing-box &>/dev/null; then
            SINGBOX_BIN="$(command -v sing-box)"
        else
            echo "Building native sing-box (make build_with_bridge)..."
            make -C "$SB_ROOT" build_with_bridge
            SINGBOX_BIN="$SB_ROOT/sing-box"
        fi
    fi
    echo "sing-box: $SINGBOX_BIN"

    # 2. Locate or build the native e2e-loadtest.
    LOADTEST_BIN="${LOADTEST_BIN:-}"
    if [ -z "$LOADTEST_BIN" ]; then
        if [ -x "$SCRIPT_DIR/loadtest/e2e-loadtest" ]; then
            LOADTEST_BIN="$SCRIPT_DIR/loadtest/e2e-loadtest"
        else
            echo "Building native e2e-loadtest..."
            ( cd "$SB_ROOT" && go build -o "$SCRIPT_DIR/loadtest/e2e-loadtest" ./e2e/loadtest/ )
            LOADTEST_BIN="$SCRIPT_DIR/loadtest/e2e-loadtest"
        fi
    fi
    echo "e2e-loadtest: $LOADTEST_BIN"

    # 3. Render the native BLE client config: substitute the peripheral ID and
    #    redirect storage to a writable temp dir (the template points at /var/lib).
    CLIENT_STORAGE="$(mktemp -d)"
    CLIENT_CONFIG="$(mktemp /tmp/sb-mixed-client-XXXXXX.json)"
    envsubst '$RNODE_BLE_CLIENT' < configs/client-ble.json \
        | sed "s|\"storage_path\":.*|\"storage_path\": \"$CLIENT_STORAGE\",|" \
        > "$CLIENT_CONFIG"

    SINGBOX_CLIENT_PID=""
    cleanup_mixed() {
        echo "=== Cleanup ==="
        [ -n "$SINGBOX_CLIENT_PID" ] && kill "$SINGBOX_CLIENT_PID" 2>/dev/null || true
        $COMPOSE_CMD -f docker-compose.mixed.yml down 2>/dev/null || true
        rm -f "$CLIENT_CONFIG"
        rm -rf "$CLIENT_STORAGE"
        echo "Logs saved in $LOG_DIR"
    }
    trap cleanup_mixed EXIT

    # 4. Bring up the container/serial server (RNode A) and wait until healthy.
    echo "Building server image..."
    $COMPOSE_CMD -f docker-compose.mixed.yml build
    echo "Starting container/serial server (RNode A on $RNODE_SERIAL_SERVER)..."
    $COMPOSE_CMD -f docker-compose.mixed.yml up -d e2e-server

    # Poll the server's healthcheck by exec'ing it into the container. Portable
    # across podman-compose (no `up --wait`) and docker compose. Up to ~120s.
    echo "Waiting for the serial server to become healthy..."
    server_ready=0
    for _ in $(seq 1 40); do
        if $COMPOSE_CMD -f docker-compose.mixed.yml exec -T e2e-server sh -c \
            'curl -sf -X POST http://localhost:8080/sum -H "Content-Type: application/json" -d "{\"a\":1,\"b\":1}" >/dev/null 2>&1 && pgrep -x sing-box >/dev/null 2>&1' \
            2>/dev/null; then
            server_ready=1
            break
        fi
        sleep 3
    done
    if [ "$server_ready" -ne 1 ]; then
        echo "Serial server did not become healthy in time — see logs/" >&2
        exit 1
    fi

    # 5. Start the native BLE client (RNode B via host BlueZ).
    echo "Starting native BLE client (RNode B = '$RNODE_BLE_CLIENT')..."
    "$SINGBOX_BIN" run -c "$CLIENT_CONFIG" > >(tee "$LOG_DIR/native-client.log") 2>&1 &
    SINGBOX_CLIENT_PID=$!

    # 6. Wait (up to 180s) for the client SOCKS proxy to accept connections.
    echo "Waiting for SOCKS 127.0.0.1:1080..."
    for _ in $(seq 1 180); do
        if (exec 3<>/dev/tcp/127.0.0.1/1080) 2>/dev/null; then
            exec 3>&- 3<&- 2>/dev/null || true
            break
        fi
        if ! kill -0 "$SINGBOX_CLIENT_PID" 2>/dev/null; then
            echo "Native client exited early — see $LOG_DIR/native-client.log" >&2
            exit 1
        fi
        sleep 1
    done

    # 7. Run the loadtest across the LoRa link; its exit code is the verdict.
    echo "=== Running e2e-loadtest over the LoRa link ==="
    rc=0
    "$LOADTEST_BIN" --socks 127.0.0.1:1080 --url http://127.0.0.1:8080 \
        --warmup-timeout 500s --concurrency 1 --requests 3 --long-terms 10000 \
        2>&1 | tee "$LOG_DIR/loadtest.log" || rc=$?
    if [ $rc -ne 0 ]; then
        echo "HW E2E mixed test FAILED (exit $rc)"
        exit 1
    fi
    ;;

  *)
    echo "Usage: $0 [serial|ble|mixed]"
    echo ""
    echo "  serial  Test RNodeSerial over USB (requires RNODE_SERIAL_SERVER, RNODE_SERIAL_CLIENT)"
    echo "  ble     Test RNodeBLE over Bluetooth (requires RNODE_BLE_SERVER, RNODE_BLE_CLIENT)"
    echo "  mixed   container/serial server ↔ LoRa ↔ native/BLE client"
    echo "          (requires RNODE_SERIAL_SERVER, RNODE_BLE_CLIENT)"
    exit 1
    ;;
esac

echo ""
echo "=== ALL HW E2E TESTS PASSED ==="
