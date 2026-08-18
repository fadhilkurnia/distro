#!/usr/bin/env bash
set -euo pipefail
# Runs on the CLIENT node (not the control node). Assumes the CLI binary
# and the target yaml under scripts/services/ have already been sent
# there by launcher.go.
#
# Env (from launcher.go):
#   SERVICE_NAME       - fixed constant "bookcatalog"
#   SERVICE_YAML_PATH  - relative path to the service yaml, ON THE CLIENT NODE
#   XDN_CONTROL_PLANE  - control node's PRIVATE IP (see launcher.go note on
#                        why this differs from the old Python reference,
#                        which used the control node's public IP)

export XDN_CONTROL_PLANE="$XDN_CONTROL_PLANE"
./bin/xdn-linux-amd64 launch "$SERVICE_NAME" --file="$SERVICE_YAML_PATH"
