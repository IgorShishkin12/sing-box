#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SING_BOX_DIR="$(dirname "$SCRIPT_DIR")"

echo "Building Rust bridge..."
cd "$SING_BOX_DIR/bridge"
cargo build --release

echo "Running E2E tests via docker-compose..."
cd "$SING_BOX_DIR"
docker compose -f docker-compose.e2e.yml build
docker compose -f docker-compose.e2e.yml up --exit-code-from e2e-client
