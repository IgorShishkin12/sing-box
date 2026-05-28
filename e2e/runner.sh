#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SING_BOX_DIR="$(dirname "$SCRIPT_DIR")"

echo "Building Rust bridge..."
cd "$SING_BOX_DIR/bridge"
cargo build --release

echo "Building e2e Docker images..."
cd "$SCRIPT_DIR"
docker compose -f docker-compose.yml build

run_profile() {
    local profile="$1"
    local exit_from="$2"
    echo ""
    echo "=== Running profile: $profile (exit-from: $exit_from) ==="
    docker compose -f docker-compose.yml --profile "$profile" up \
        --exit-code-from "$exit_from" \
        --abort-on-container-exit
    docker compose -f docker-compose.yml --profile "$profile" down --volumes
}

run_profile tcp  e2e-client
run_profile udp  e2e-client-udp
run_profile auto e2e-client-auto

echo ""
echo "=== ALL E2E PROFILES PASSED ==="
