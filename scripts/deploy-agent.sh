#!/usr/bin/env bash
# Deploy and start sentinel-agent on one or more remote hosts via systemd.
#
# Uses the binary produced by scripts/build.sh (dist/sentinel-agent), so run
# that first. Real eBPF mode is the default; pass --mock for hosts without
# CAP_BPF or for a quick end-to-end smoke test.
#
# Usage:
#   ./scripts/deploy-agent.sh [OPTIONS] HOST...
#
# Examples:
#   ./scripts/deploy-agent.sh --collector 192.168.68.63:50051 yuki-private ds1
#   ./scripts/deploy-agent.sh --mock --collector 192.168.68.63:50051 minipc
#   ./scripts/deploy-agent.sh --node-id web-01 --collector 192.168.68.63:50051 minipc
#
# Requirements:
#   - dist/sentinel-agent built locally (scripts/build.sh)
#   - SSH key-based auth to all HOSTs
#   - sudo access on HOSTs (for systemd unit install)
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
COLLECTOR_ADDR="${SENTINEL_COLLECTOR:-192.168.68.63:50051}"
MOCK=false
MOCK_RATE=3
NODE_ID_OVERRIDE=""

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$REPO_ROOT/dist/sentinel-agent"
REMOTE_BIN="/usr/local/bin/sentinel-agent"
SYSTEMD_DIR="/etc/systemd/system"

# ── Arg parsing ───────────────────────────────────────────────────────────────
HOSTS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --collector) COLLECTOR_ADDR="$2"; shift 2 ;;
    --mock)      MOCK=true; shift ;;
    --mock-rate) MOCK_RATE="$2"; shift 2 ;;
    --node-id)   NODE_ID_OVERRIDE="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,19p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    -*) echo "Unknown option: $1" >&2; exit 1 ;;
    *)  HOSTS+=("$1"); shift ;;
  esac
done

if [[ ${#HOSTS[@]} -eq 0 ]]; then
  echo "Error: no target hosts specified" >&2
  echo "Usage: $0 [OPTIONS] HOST..." >&2
  exit 1
fi

if [[ -n "$NODE_ID_OVERRIDE" && ${#HOSTS[@]} -gt 1 ]]; then
  echo "Error: --node-id can only be used with a single host" >&2
  exit 1
fi

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found at $BINARY. Run scripts/build.sh first." >&2
  exit 1
fi

# ── Systemd unit template ─────────────────────────────────────────────────────
make_unit() {
  local node_id="$1"
  local extra_flags=""
  if $MOCK; then
    extra_flags=" --mock --mock-rate ${MOCK_RATE}"
  fi

  cat <<UNIT
[Unit]
Description=SentinelMesh Agent (${node_id})
After=network.target
Wants=network.target

[Service]
Type=simple
ExecStart=${REMOTE_BIN} --collector http://${COLLECTOR_ADDR} --node-id ${node_id}${extra_flags}
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=sentinel-agent

[Install]
WantedBy=multi-user.target
UNIT
}

# ── Deploy to each host ───────────────────────────────────────────────────────
deploy_to() {
  local host="$1"
  local node_id="$NODE_ID_OVERRIDE"
  if [[ -z "$node_id" ]]; then
    node_id="$(ssh "$host" hostname -s 2>/dev/null || echo "$host")"
  fi

  echo ""
  echo "=== deploying agent to ${host} (node_id=${node_id}, mock=${MOCK}) ==="

  echo "[${host}] uploading binary ..."
  scp "$BINARY" "${host}:/tmp/sentinel-agent"
  ssh "$host" "sudo install -m 755 /tmp/sentinel-agent ${REMOTE_BIN} && rm -f /tmp/sentinel-agent"

  echo "[${host}] installing systemd unit ..."
  make_unit "$node_id" | ssh "$host" "sudo tee ${SYSTEMD_DIR}/sentinel-agent.service > /dev/null"

  echo "[${host}] enabling and starting service ..."
  ssh "$host" "sudo systemctl daemon-reload && \
               sudo systemctl enable sentinel-agent && \
               sudo systemctl restart sentinel-agent"

  echo "[${host}] service status:"
  ssh "$host" "systemctl is-active sentinel-agent && \
               journalctl -u sentinel-agent -n 5 --no-pager" || true
}

for host in "${HOSTS[@]}"; do
  deploy_to "$host"
done

echo ""
echo "=== deploy complete ==="
echo "Collector API: http://${COLLECTOR_ADDR%:*}:8081/api/nodes"
echo "Check all nodes active with:"
echo "  curl -s http://${COLLECTOR_ADDR%:*}:8081/api/nodes | jq '.[] | {node_id, active}'"
