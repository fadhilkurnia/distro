#!/usr/bin/env bash
set -euo pipefail
# Runs on the client node. Sends the placement change to the control
# plane and prints the raw response body, so the caller can tell success
# from failure.
#
# Env (from launcher.go):
#   XDN_CONTROL_PLANE - control node's PRIVATE IP
#   SERVICE_NAME      - fixed constant "bookcatalog"
#   PLACEMENT_BODY    - the JSON body to send, built by launcher.go

curl -sS -w "\nHTTP_STATUS:%{http_code}" \
  -X PUT "http://${XDN_CONTROL_PLANE}:3300/api/v2/services/${SERVICE_NAME}/placement" \
  -H "Content-Type: application/json" \
  -H "XDN: ${SERVICE_NAME}" \
  -d "${PLACEMENT_BODY}"
