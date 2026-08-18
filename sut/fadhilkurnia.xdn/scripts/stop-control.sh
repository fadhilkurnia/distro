#!/usr/bin/env bash
set -euo pipefail
# Runs ONCE on the control node, mirroring start-control.sh. gpServer.sh's
# own internal SSH reaches every other node itself to kill processes and
# clean /tmp/xdn, /dev/shm/xdn — confirmed from source (bin/gpServer.sh),
# not assumed.
#
# Env (from launcher.go):
#   CONFIG_PATH  - path to run_config.properties, relative to this
#                  script's starting CWD (l.WorkDir on the control node)
#   SSH_KEY_PATH - path to the distributed key, ON THE CONTROL NODE

ABS_CONFIG_PATH="$(realpath "$CONFIG_PATH")"
export SSH_KEY_PATH

cd repo
bash bin/gpServer.sh -DgigapaxosConfig="$ABS_CONFIG_PATH" forceclear all
sudo -n rm -rf /tmp/xdn || echo "note: could not remove /tmp/xdn (no passwordless sudo here. Fine for local testing, expected to work on CloudLab)"
