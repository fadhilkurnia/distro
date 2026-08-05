#!/usr/bin/env bash
set -euo pipefail
echo "[dummy] ${MESSAGE:-hello}"
echo "[dummy] nix-provisioned: $(hello)"
