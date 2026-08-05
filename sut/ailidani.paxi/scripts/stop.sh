#!/usr/bin/env bash
set -euo pipefail
# HASH is passed in by launcher.go via env

BIN=".build/$HASH/bin/server"

pids=$(ps aux | grep "$BIN" | grep -v grep | awk '{print $2}' || true)
for pid in $pids; do
  echo "Killing $pid"
  kill -9 "$pid" 2>/dev/null || true
done

rm -f ".build/$HASH/run_config.json"
