#!/usr/bin/env bash
# Deploy sentinel-agent to remote nodes.
#
# Usage:
#   ./scripts/deploy-agent.sh [OPTIONS] HOST...
#
# Examples:
#   ./scripts/deploy-agent.sh yuki-private ds1
#   ./scripts/deploy-agent.sh --collector 192.168.68.63:50051 yuki-private ds1
#   ./scripts/deploy-agent.sh --mock yuki-private
#
# Requirements:
#   - cargo cross OR cargo with musl target installed locally
#   - SSH key-based auth to all HOSTs
#   - sudo access on HOSTs (for systemd unit install)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
COLLECTOR_ADDR="${SENTINEL_COLLECTOR:-192.168.68.63:50051}"
MOCK=false
MOCK_RATE=2
TARGET="x86_64-unknown-linux-musl"
BINARY_NAME="sentinel-agent"
INSTALL_DIR="/usr/local/bin"
SYSTEMD_DIR="/etc/systemd/system"
AGENT_DIR="$(cd "$(dirname "$0")/../rust-agent" && pwd)"

# ── Arg parsing ───────────────────────────────────────────────────────────────
HOSTS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --collector) COLLECTOR_ADDR="$2"; shift 2 ;;
    --mock)      MOCK=true; shift ;;
    --mock-rate) MOCK_RATE="$2"; shift 2 ;;
    --target)    TARGET="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,10p' "$0" | sed 's/^# \?//'
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

# ── Build ─────────────────────────────────────────────────────────────────────
echo "[build] compiling sentinel-agent for ${TARGET} ..."
(
  cd "$AGENT_DIR"
  if command -v cross &>/dev/null; then
    cross build --release --bin sentinel-agent --target "$TARGET"
  else
    cargo build --release --bin sentinel-agent --target "$TARGET"
  fi
)

BINARY="$AGENT_DIR/target/${TARGET}/release/${BINARY_NAME}"
if [[ ! -f "$BINARY" ]]; then
  echo "Error: binary not found at $BINARY" >&2
  exit 1
fi

echo "[build] binary: $BINARY ($(du -sh "$BINARY" | cut -f1))"

# ── Systemd unit template ─────────────────────────────────────────────────────
make_unit() {
  local node_id="$1"
  local extra_flags=""
  if $MOCK; then
    extra_flags="--mock --mock-rate ${MOCK_RATE}"
  fi

  cat <<UNIT
[Unit]
Description=SentinelMesh Agent (${node_id})
After=network.target
Wants=network.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/${BINARY_NAME} \\
  --collector http://${COLLECTOR_ADDR} \\
  --node-id ${node_id} \\
  ${extra_flags}
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
  local node_id
  node_id="$(ssh "$host" hostname -s 2>/dev/null || echo "$host")"
  echo ""
  echo "=== deploying to ${host} (node_id=${node_id}) ==="

  # Upload binary
  echo "[${host}] uploading binary ..."
  scp "$BINARY" "${host}:/tmp/${BINARY_NAME}"
  ssh "$host" "sudo install -m 755 /tmp/${BINARY_NAME} ${INSTALL_DIR}/${BINARY_NAME}"

  # Write systemd unit
  echo "[${host}] installing systemd unit ..."
  make_unit "$node_id" | ssh "$host" "sudo tee ${SYSTEMD_DIR}/sentinel-agent.service > /dev/null"

  # Enable and (re)start
  echo "[${host}] enabling and starting service ..."
  ssh "$host" "sudo systemctl daemon-reload && \
               sudo systemctl enable sentinel-agent && \
               sudo systemctl restart sentinel-agent"

  # Show status
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
