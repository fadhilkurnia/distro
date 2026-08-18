#!/usr/bin/env bash
set -euo pipefail
# Runs ONCE on the control node. gpServer.sh's own internal SSH/rsync
# handles distributing to every other replica and the reconfigurator —
# we don't loop over nodes ourselves here.
#
# Env (from launcher.go):
#   CONFIG_PATH  - path to run_config.properties, relative to this
#                  script's starting CWD (l.WorkDir on the control node)
#   SSH_KEY_PATH - path to the distributed key, ON THE CONTROL NODE

# Resolve to absolute BEFORE cd'ing into repo/ — gpServer.sh's own
# relative lookups are relative to ITS OWN cwd, so -DgigapaxosConfig
# needs to be unambiguous regardless of where it resolves paths from.
ABS_CONFIG_PATH="$(realpath "$CONFIG_PATH")"
export SSH_KEY_PATH

cd repo
LOG_FILE=".build/gpserver-start.log"
mkdir -p "$(dirname "$LOG_FILE")"
bash bin/gpServer.sh -DgigapaxosConfig="$ABS_CONFIG_PATH" start all > "$LOG_FILE" 2>&1
echo "gpServer.sh finished; server output redirected to $LOG_FILE"
