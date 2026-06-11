#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

COMPOSE_CMD="docker compose"
if command -v podman-compose &>/dev/null; then
    COMPOSE_CMD="podman-compose"
elif podman compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="podman compose"
fi
echo "Using compose: $COMPOSE_CMD"

cd "$SCRIPT_DIR"

mkdir -p bench-results

echo "Building benchmark images..."
$COMPOSE_CMD -f docker-compose.bench-reticulum.yml build

run_bench() {
    local file="$1"
    local exit_from="$2"
    echo ""
    echo "=== Running: $file ==="
    $COMPOSE_CMD -f "$file" up \
        --exit-code-from "$exit_from" \
        --abort-on-container-exit
    $COMPOSE_CMD -f "$file" down
}

run_bench docker-compose.bench-baseline.yml   bench-client
run_bench docker-compose.bench-reticulum.yml  bench-client

echo ""
echo "================================================================"
echo "BASELINE (direct HTTP, no proxy)"
echo "================================================================"
cat bench-results/baseline.txt

echo ""
echo "================================================================"
echo "RETICULUM (SOCKS5 → reticulum tunnel)"
echo "================================================================"
cat bench-results/reticulum.txt

echo ""
echo "=== BENCHMARKS COMPLETE ==="
