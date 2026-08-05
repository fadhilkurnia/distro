#!/usr/bin/env bash
set -euo pipefail
# HASH is passed in by launcher.go via env.

# Paxi's own repo already has a build.sh under bin/ that produces a
# "server" binary in that same directory. We just run it, then copy 
# the result out into this version's own .build/<hash>/bin/ directory
cd repo/bin
./build.sh

mkdir -p "../../.build/$HASH/bin"
cp server "../../.build/$HASH/bin/server"
