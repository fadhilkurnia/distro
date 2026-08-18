#!/usr/bin/env bash
set -euo pipefail
# Run ONCE on the control node (nodes[0]) as part of every Build, before
# any XDN-specific compilation happens. Idempotent: every step checks
# current state first and skips if already satisfied, so repeat runs are
# cheap no-ops. Ported from xdnd's install_dependencies/init_docker_swarm,
# with idempotency checks added (the vendor script reinstalls unconditionally
# every time, which we don't want for a per-Build preamble).
#
# Env (from launcher.go):
#   SSH_KEY_PATH   - path to the distributed key, ON THIS MACHINE
#   SSH_USERNAME   - username to use for every node-to-node hop
#   REPLICA_ADDRS  - comma-separated private IPs of every replica node
#   CLIENT_ADDR    - private IP of the client/reconfigurator node

ALL_ADDRS="${REPLICA_ADDRS},${CLIENT_ADDR}"
IFS=',' read -ra NODES <<< "$ALL_ADDRS"

ssh_to() {
  local addr="$1"; shift
  ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=accept-new -o BatchMode=yes \
    "$SSH_USERNAME@$addr" "$@"
}

echo "Checking passwordless sudo on every node..."
for addr in "${NODES[@]}"; do
  if ! ssh_to "$addr" "sudo -n true" 2>/dev/null; then
    echo "passwordless sudo not available for $SSH_USERNAME@$addr" >&2
    exit 1
  fi
done

echo "Ensuring Docker + FUSE are installed on every node (idempotent)..."
for addr in "${NODES[@]}"; do
  ssh_to "$addr" '
    set -e
    if ! command -v docker >/dev/null 2>&1; then
      echo "  [$(hostname)] installing docker..."
      sudo apt-get update -y -q
      sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q ca-certificates curl
      sudo install -m 0755 -d /etc/apt/keyrings
      sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
      sudo chmod a+r /etc/apt/keyrings/docker.asc
      echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      sudo apt-get update -y -q
      sudo apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    fi

    if ! id -nG "$USER" | grep -qw docker; then
      echo "  [$(hostname)] adding $USER to docker group..."
      sudo groupadd -f docker
      sudo usermod -aG docker "$USER"
    fi

    if ! dpkg -s libfuse3-dev >/dev/null 2>&1; then
      echo "  [$(hostname)] installing libfuse3-dev..."
      sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q libfuse3-dev
    fi
  '
done

echo "Ensuring Docker Swarm membership (idempotent, first node is manager)..."
MANAGER="${NODES[0]}"
if ! ssh_to "$MANAGER" '
  state=$(sudo docker info --format "{{.Swarm.LocalNodeState}}" 2>/dev/null || echo unknown)
  [ "$state" = "active" ]
'; then
  echo "  init swarm on $MANAGER..."
  ssh_to "$MANAGER" "sudo docker swarm init --advertise-addr $MANAGER >/dev/null"
fi

TOKEN=$(ssh_to "$MANAGER" "sudo docker swarm join-token worker -q")
if [ -z "$TOKEN" ]; then
  echo "could not retrieve swarm join token from $MANAGER" >&2
  exit 1
fi

for ((i = 1; i < ${#NODES[@]}; i++)); do
  addr="${NODES[$i]}"
  if ! ssh_to "$addr" '
    state=$(sudo docker info --format "{{.Swarm.LocalNodeState}}" 2>/dev/null || echo unknown)
    [ "$state" = "active" ]
  '; then
    echo "  join $addr..."
    ssh_to "$addr" "sudo docker swarm join --token $TOKEN $MANAGER:2377 >/dev/null"
  fi
done

echo "Cluster provisioning check complete."
