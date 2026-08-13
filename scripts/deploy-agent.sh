#!/usr/bin/env bash
# Deploy and start sentinel-agent (mock mode) on a remote host.
# Usage: deploy-agent.sh <SSH_HOST> <NODE_ID> <COLLECTOR_ADDR>
# Example: deploy-agent.sh minipc web-01 192.168.68.63:50051
set -euo pipefail

SSH_HOST="${1:?Usage: $0 <SSH_HOST> <NODE_ID> <COLLECTOR_ADDR>}"
NODE_ID="${2:?NODE_ID required}"
COLLECTOR_ADDR="${3:?COLLECTOR_ADDR required (host:port)}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$REPO_ROOT/dist/sentinel-agent"
REMOTE_BIN="/usr/local/bin/sentinel-agent"

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found. Run scripts/build.sh first."
  exit 1
fi

echo "==> Deploying agent to $SSH_HOST (node-id: $NODE_ID)..."
scp "$BINARY" "$SSH_HOST:/tmp/sentinel-agent"
ssh "$SSH_HOST" "sudo mv /tmp/sentinel-agent $REMOTE_BIN && sudo chmod +x $REMOTE_BIN"

SERVICE="sentinel-agent-${NODE_ID}"
echo "==> Installing systemd service: $SERVICE..."
ssh "$SSH_HOST" "sudo tee /etc/systemd/system/${SERVICE}.service > /dev/null" <<EOF
[Unit]
Description=SentinelMesh Agent ($NODE_ID)
After=network.target

[Service]
ExecStart=$REMOTE_BIN --collector http://$COLLECTOR_ADDR --node-id $NODE_ID --mock --mock-rate 3
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

ssh "$SSH_HOST" "sudo systemctl daemon-reload && sudo systemctl enable --now $SERVICE"
echo "==> Agent '$NODE_ID' running on $SSH_HOST → $COLLECTOR_ADDR"
