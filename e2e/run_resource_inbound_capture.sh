#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="/home/alice/code/reticulum-v2/sing-box/e2e/docker-compose.internet-proxy.yml"
LOG_FILE="/home/alice/code/reticulum-v2/sing-box/e2e/logs/e2e.log"
OUTPUT_FILE="/home/alice/code/reticulum-v2/sing-box/e2e/logs/resource_inbound_captures.log"
CONTEXT_LINES=50
RUNS=7
UP_SECONDS=60

> "$OUTPUT_FILE"

for i in $(seq 1 $RUNS); do
    echo "=== Run $i/$RUNS ===" | tee -a "$OUTPUT_FILE"

    podman-compose -f "$COMPOSE_FILE" up -d || true

    echo "Waiting ${UP_SECONDS}s..."
    sleep "$UP_SECONDS"

    LINE_NUM=$(grep -n "resource inbound.*total=65600" "$LOG_FILE" 2>/dev/null | tail -1 | cut -d: -f1)

    if [ -n "$LINE_NUM" ]; then
        START=$(( LINE_NUM - CONTEXT_LINES ))
        END=$(( LINE_NUM + CONTEXT_LINES ))
        [ "$START" -lt 1 ] && START=1

        echo "--- Matched at line $LINE_NUM (showing lines $START-$END) ---" >> "$OUTPUT_FILE"
        sed -n "${START},${END}p" "$LOG_FILE" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        echo "Captured lines $START-$END around line $LINE_NUM"
    else
        echo "WARNING: No 'resource inbound.*total=65600' line found in run $i" | tee -a "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
    fi

    podman-compose -f "$COMPOSE_FILE" down || true

    > "$LOG_FILE"
    echo "Log cleared."
done

echo "Done. Results in $OUTPUT_FILE"
