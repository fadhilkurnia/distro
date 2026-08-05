#!/usr/bin/env bash
set -euo pipefail
# HASH, NODE_ID, ALGORITHM are passed in by launcher.go via env

BIN=".build/$HASH/bin/server"
CONFIG=".build/$HASH/run_config.json"

nohup "$BIN" -id "$NODE_ID" -algorithm="$ALGORITHM" -config "$CONFIG" > /dev/null 2>&1 &
