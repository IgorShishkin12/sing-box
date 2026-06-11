#!/usr/bin/env bash
# Hardware E2E test runner for RNodeSerial and RNodeBLE interfaces.
# Requires two physical RNode devices connected to the host.
# Not run in CI — invoke manually with the appropriate env vars set.
#
# Usage:
#   ./runner-hw.sh serial   # RNodeSerial over USB
#   ./runner-hw.sh ble      # RNodeBLE over Bluetooth
#
# Environment variables (serial):
#   RNODE_SERIAL_SERVER  Host device path for server-side RNode (default: /dev/ttyUSB0)
#   RNODE_SERIAL_CLIENT  Host device path for client-side RNode (default: /dev/ttyUSB1)
#
# Environment variables (BLE):
#   RNODE_BLE_SERVER     BLE peripheral ID (name or MAC) for server-side RNode
#   RNODE_BLE_CLIENT     BLE peripheral ID (name or MAC) for client-side RNode
#
# Both RNodes must be tuned to the same LoRa parameters (868 MHz / 125 kHz BW /
# SF9 / CR 4/5 by default; edit configs to change).

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

    # Inject peripheral IDs into config templates, producing resolved copies
    # that docker-compose.ble.yml mounts into the containers.
    envsubst '$RNODE_BLE_SERVER' < configs/server-ble.json > configs/server-ble-resolved.json
    envsubst '$RNODE_BLE_CLIENT' < configs/client-ble.json > configs/client-ble-resolved.json

    echo "Building e2e images..."
    $COMPOSE_CMD -f docker-compose.ble.yml build

    run_test docker-compose.ble.yml

    rm -f configs/server-ble-resolved.json configs/client-ble-resolved.json
    ;;

  *)
    echo "Usage: $0 [serial|ble]"
    echo ""
    echo "  serial  Test RNodeSerial over USB (requires RNODE_SERIAL_SERVER, RNODE_SERIAL_CLIENT)"
    echo "  ble     Test RNodeBLE over Bluetooth (requires RNODE_BLE_SERVER, RNODE_BLE_CLIENT)"
    exit 1
    ;;
esac

echo ""
echo "=== ALL HW E2E TESTS PASSED ==="
