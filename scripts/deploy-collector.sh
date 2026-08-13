#!/usr/bin/env bash
# Deploy sentinel-collector to a host and install as a systemd service.
#
# Usage:
#   ./scripts/deploy-collector.sh [OPTIONS] HOST
#
# Examples:
#   ./scripts/deploy-collector.sh minipc
#   ./scripts/deploy-collector.sh --grpc-addr :50051 --http-addr :8081 minipc
#
# Requirements:
#   - Go 1.22+ installed locally for build
#   - SSH key-based auth to HOST
#   - sudo access on HOST (for systemd unit install)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
GRPC_ADDR=":50051"
HTTP_ADDR=":8081"
DB_PATH="/var/lib/sentinel-collector/events.db"
RULES_PATH="/etc/sentinel-collector/rules.yaml"
BINARY_NAME="sentinel-collector"
INSTALL_DIR="/usr/local/bin"
SYSTEMD_DIR="/etc/systemd/system"
COLLECTOR_DIR="$(cd "$(dirname "$0")/../go-collector" && pwd)"

# ── Arg parsing ───────────────────────────────────────────────────────────────
HOST=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --grpc-addr)  GRPC_ADDR="$2";  shift 2 ;;
    --http-addr)  HTTP_ADDR="$2";  shift 2 ;;
    --db)         DB_PATH="$2";    shift 2 ;;
    --rules)      RULES_PATH="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,10p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    -*) echo "Unknown option: $1" >&2; exit 1 ;;
    *)
      if [[ -n "$HOST" ]]; then
        echo "Error: only one target host supported" >&2; exit 1
      fi
      HOST="$1"; shift ;;
  esac
done

if [[ -z "$HOST" ]]; then
  echo "Error: no target host specified" >&2
  echo "Usage: $0 [OPTIONS] HOST" >&2
  exit 1
fi

# ── Build ─────────────────────────────────────────────────────────────────────
echo "[build] compiling sentinel-collector (linux/amd64) ..."
(
  cd "$COLLECTOR_DIR"
  GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o "/tmp/${BINARY_NAME}" \
    ./cmd/collector
)

echo "[build] binary: /tmp/${BINARY_NAME} ($(du -sh "/tmp/${BINARY_NAME}" | cut -f1))"

# ── Systemd unit ──────────────────────────────────────────────────────────────
UNIT_CONTENT="[Unit]
Description=SentinelMesh Collector
After=network.target
Wants=network.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/${BINARY_NAME} serve \\
  --grpc-addr ${GRPC_ADDR} \\
  --http-addr ${HTTP_ADDR} \\
  --db ${DB_PATH} \\
  --rules ${RULES_PATH}
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=sentinel-collector

[Install]
WantedBy=multi-user.target"

# ── Deploy ────────────────────────────────────────────────────────────────────
echo ""
echo "=== deploying collector to ${HOST} ==="

# Upload binary
echo "[${HOST}] uploading binary ..."
scp "/tmp/${BINARY_NAME}" "${HOST}:/tmp/${BINARY_NAME}"
ssh "$HOST" "sudo install -m 755 /tmp/${BINARY_NAME} ${INSTALL_DIR}/${BINARY_NAME}"

# Create directories
echo "[${HOST}] creating data directories ..."
ssh "$HOST" "sudo mkdir -p $(dirname "$DB_PATH") $(dirname "$RULES_PATH")"

# Upload rules.yaml
RULES_LOCAL="$(cd "$(dirname "$0")/.." && pwd)/rules.yaml"
if [[ -f "$RULES_LOCAL" ]]; then
  echo "[${HOST}] uploading rules.yaml ..."
  scp "$RULES_LOCAL" "${HOST}:/tmp/rules.yaml"
  ssh "$HOST" "sudo cp /tmp/rules.yaml ${RULES_PATH}"
else
  echo "[${HOST}] warning: rules.yaml not found locally, skipping"
fi

# Write systemd unit
echo "[${HOST}] installing systemd unit ..."
echo "$UNIT_CONTENT" | ssh "$HOST" "sudo tee ${SYSTEMD_DIR}/sentinel-collector.service > /dev/null"

# Enable and (re)start
echo "[${HOST}] enabling and starting service ..."
ssh "$HOST" "sudo systemctl daemon-reload && \
             sudo systemctl enable sentinel-collector && \
             sudo systemctl restart sentinel-collector"

# Show status
echo "[${HOST}] service status:"
ssh "$HOST" "systemctl is-active sentinel-collector && \
             journalctl -u sentinel-collector -n 5 --no-pager" || true

echo ""
echo "=== deploy complete ==="
echo "REST API: http://$(ssh "$HOST" hostname -I | awk '{print $1}' 2>/dev/null || echo "$HOST")${HTTP_ADDR}"
echo "gRPC:     $(ssh "$HOST" hostname -I | awk '{print $1}' 2>/dev/null || echo "$HOST")${GRPC_ADDR}"
