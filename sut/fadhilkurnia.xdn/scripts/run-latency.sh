#!/usr/bin/env bash
set -euo pipefail
# Same shape as Paxi's run-latency.sh: WARMUP_DURATION, DURATION,
# WRITE_RATIO, REQUEST_INTERVAL are read directly by latency.js via
# __ENV; this wrapper just runs k6 and writes the summary JSON.

k6 run --out json="$RESULT_PATH" "$SCRIPT_PATH"
