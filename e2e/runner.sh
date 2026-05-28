#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Support both Docker Compose and Podman Compose.
# podman-compose v1.x does not support --profile, so we use per-transport files.
COMPOSE_CMD="docker compose"
if command -v podman-compose &>/dev/null; then
    COMPOSE_CMD="podman-compose"
elif podman compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="podman compose"
fi
echo "Using compose: $COMPOSE_CMD"

cd "$SCRIPT_DIR"

echo "Building e2e images..."
$COMPOSE_CMD -f docker-compose.tcp.yml build

run_test() {
    local file="$1"
    local exit_from="$2"
    echo ""
    echo "=== Running: $file (exit-from: $exit_from) ==="
    $COMPOSE_CMD -f "$file" up \
        --exit-code-from "$exit_from" \
        --abort-on-container-exit
    $COMPOSE_CMD -f "$file" down --volumes
}

run_test docker-compose.tcp.yml  e2e-client
run_test docker-compose.udp.yml  e2e-client-udp
run_test docker-compose.auto.yml e2e-client-auto

echo ""
echo "=== ALL E2E TESTS PASSED ==="
