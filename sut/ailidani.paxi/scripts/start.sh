#!/usr/bin/env bash
set -euo pipefail
# HASH, NODE_ID, ALGORITHM are passed in by launcher.go via env.

BIN=".build/$HASH/bin/server"
CONFIG=".build/$HASH/run_config.json"
LOG=".build/$HASH/server.$NODE_ID.log"

# Redirect the server's own stdout/stderr to a log file
nohup "$BIN" -id "$NODE_ID" -algorithm="$ALGORITHM" -config "$CONFIG" -log_dir=".build/$HASH" > "$LOG" 2>&1 &

# Give the process a moment to either settle or crash, then explicitly
# confirm it's actually still alive rather than reporting success just
# because backgrounding the command itself didn't error.
sleep 1
if ! kill -0 "$!" 2>/dev/null; then
  echo "server process exited immediately after starting — see $LOG:" >&2
  cat "$LOG" >&2
  exit 1
fi
