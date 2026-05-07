#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SING_BOX_DIR="$(dirname "$SCRIPT_DIR")"

echo "Building Rust bridge..."
cd "$SING_BOX_DIR/bridge"
cargo build

echo "Setting LD_LIBRARY_PATH..."
export LD_LIBRARY_PATH="$SING_BOX_DIR/bridge/target/debug:$LD_LIBRARY_PATH"

echo "Running E2E tests..."
cd "$SING_BOX_DIR"
go test -v -count=1 ./e2e/...