#!/usr/bin/env bash
set -euo pipefail
set -x  # trace every command as it runs, so nothing is ambiguous
# HASH is passed in by launcher.go via env.

mkdir -p ".build/$HASH/bin"
cd repo/server
go build -o "../../.build/$HASH/bin/server" .

# Explicit, unambiguous confirmation of what was actually produced —
# no more inferring success from exit code alone.
ls -la "../../.build/$HASH/bin/server"
