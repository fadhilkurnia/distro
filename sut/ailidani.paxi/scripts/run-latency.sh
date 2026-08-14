#!/usr/bin/env bash
set -euo pipefail
# SCRIPT_PATH, RESULT_PATH are passed as env vars by launcher.go.
# WARMUP_DURATION, DURATION, WRITE_RATIO, and per-scenario ADDR values are
# read directly by the k6 script itself via __ENV — this wrapper only
# needs to know which script to run and where to write the summary.

k6 run --out json="$RESULT_PATH" "$SCRIPT_PATH"
