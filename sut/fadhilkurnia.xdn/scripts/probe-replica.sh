#!/usr/bin/env bash
set -euo pipefail
# Runs on the client node. Sends a single real request straight to one
# replica's own address, to check whether that replica is actually
# serving yet. Prints the HTTP status code on its own line so the caller
# can tell a real response apart from curl failing to connect at all.
#
# Env (from launcher.go):
#   TARGET_ADDR  - "host:port" of the replica being checked
#   PROBE_PATH   - path to request, e.g. "/api/books"

curl -sS -o /dev/null -w "HTTP_STATUS:%{http_code}" \
  --max-time 5 \
  "http://${TARGET_ADDR}${PROBE_PATH}"
