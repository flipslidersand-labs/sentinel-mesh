#!/usr/bin/env bash
# Deploy and start sentinel-collector on a remote host via systemd.
# Usage: install-collector.sh <SSH_HOST> [--grpc-addr :50051] [--http-addr :8081]
set -euo pipefail

SSH_HOST="${1:?Usage: $0 <SSH_HOST> [options]}"
shift

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$REPO_ROOT/dist"
BINARY="$DIST/sentinel-collector"
REMOTE_BIN="/usr/local/bin/sentinel-collector"
REMOTE_STATIC="/opt/sentinel-collector/static"
STATIC_DIR="$REPO_ROOT/go-collector/static"

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found. Run scripts/build.sh first."
  exit 1
fi

echo "==> Deploying collector to $SSH_HOST..."
ssh "$SSH_HOST" "sudo mkdir -p /opt/sentinel-collector/static"
scp "$BINARY" "$SSH_HOST:/tmp/sentinel-collector"
ssh "$SSH_HOST" "sudo mv /tmp/sentinel-collector $REMOTE_BIN && sudo chmod +x $REMOTE_BIN"

if [[ -d "$STATIC_DIR" ]]; then
  echo "==> Uploading UI static files..."
  scp -r "$STATIC_DIR/." "$SSH_HOST:/tmp/sentinel-static/"
  ssh "$SSH_HOST" "sudo rsync -a /tmp/sentinel-static/ $REMOTE_STATIC/ && rm -rf /tmp/sentinel-static"
fi

echo "==> Installing systemd service..."
ssh "$SSH_HOST" "sudo tee /etc/systemd/system/sentinel-collector.service > /dev/null" <<EOF
[Unit]
Description=SentinelMesh Collector
After=network.target

[Service]
ExecStart=$REMOTE_BIN serve --grpc-addr :50051 --http-addr :8081 --data-dir /var/lib/sentinel --static-dir $REMOTE_STATIC $*
WorkingDirectory=/opt/sentinel-collector
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

ssh "$SSH_HOST" "sudo systemctl daemon-reload && sudo systemctl enable --now sentinel-collector"
echo "==> sentinel-collector running on $SSH_HOST :50051 (gRPC) :8081 (HTTP)"
echo "    Dashboard: http://$(ssh "$SSH_HOST" 'hostname -I | awk "{print \$1}"'):8081"
