#!/usr/bin/env bash
set -euo pipefail
set -x
# Runs locally (on the orchestrator). Same convention as Paxi's build.sh:
# the repo lives at ./repo relative to this script's starting CWD
# (l.WorkDir), and this script cd's into it itself.

cd repo
bash bin/build_xdn_jar.sh
bash bin/build_xdn_cli.sh

# Confirm what actually got produced, same philosophy as Paxi's build.sh.
ls -la jars/
ls -la bin/xdn-linux-amd64
