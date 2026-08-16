#!/usr/bin/env bash
# Multi-region demo: two region collectors + a cross-region aggregator.
#
# Topology:
#   minipc  — region collector "asia-west"  :50051 gRPC  :8081 HTTP
#   yuki    — region collector "asia-east"  :50052 gRPC  :8082 HTTP
#   minipc  — aggregator (polls both)       :8083 HTTP   (no gRPC)
#
# Each region collector receives agents in its own region.
# The aggregator merges /api/nodes,events,alerts,regions from both collectors.
#
# Usage:
#   ./scripts/demo-multi-region.sh [--mock]
#
# Requirements:
#   - scripts/build.sh already run
#   - SSH key access to minipc and yuki-private
#   - sudo on both hosts
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$REPO_ROOT/scripts"

MOCK=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mock) MOCK=true; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

MOCK_FLAG=""
if $MOCK; then MOCK_FLAG="--mock"; fi

# ── Host / address config ──────────────────────────────────────────────────────
MINIPC_HOST="minipc"
MINIPC_IP="192.168.68.63"
YUKI_HOST="yuki-private"
YUKI_IP="192.168.68.56"

WEST_GRPC="${MINIPC_IP}:50051"
WEST_HTTP="http://${MINIPC_IP}:8081"
EAST_GRPC="${YUKI_IP}:50052"
EAST_HTTP="http://${YUKI_IP}:8082"
AGG_HTTP="http://${MINIPC_IP}:8083"

# ── Step 1: build ──────────────────────────────────────────────────────────────
echo "==> Step 1: Build binaries"
bash "$SCRIPTS/build.sh"

# ── Step 2: Deploy region collectors ──────────────────────────────────────────
echo ""
echo "==> Step 2a: Deploy asia-west collector → $MINIPC_HOST"
bash "$SCRIPTS/install-collector.sh" "$MINIPC_HOST" \
  --region asia-west --grpc-addr :50051 --http-addr :8081

if ssh -o ConnectTimeout=5 -o BatchMode=yes "$YUKI_HOST" true 2>/dev/null; then
  echo ""
  echo "==> Step 2b: Deploy asia-east collector → $YUKI_HOST"
  bash "$SCRIPTS/install-collector.sh" "$YUKI_HOST" \
    --region asia-east --grpc-addr :50052 --http-addr :8082
else
  echo "==> Step 2b: $YUKI_HOST unreachable — skipping asia-east collector"
  EAST_HTTP=""
fi

# ── Step 3: Deploy aggregator on minipc ───────────────────────────────────────
echo ""
echo "==> Step 3: Deploy aggregator → $MINIPC_HOST :8083"

UPSTREAMS="asia-west=${WEST_HTTP}"
if [[ -n "$EAST_HTTP" ]]; then
  UPSTREAMS="${UPSTREAMS},asia-east=${EAST_HTTP}"
fi

ssh "$MINIPC_HOST" "sudo tee /etc/systemd/system/sentinel-aggregator.service > /dev/null" <<EOF
[Unit]
Description=SentinelMesh Aggregator
After=network.target sentinel-collector.service

[Service]
ExecStart=/usr/local/bin/sentinel-collector serve --aggregate --http-addr :8083 --upstreams ${UPSTREAMS} --poll-interval 10s
WorkingDirectory=/opt/sentinel-collector
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
ssh "$MINIPC_HOST" "sudo systemctl daemon-reload && sudo systemctl enable --now sentinel-aggregator"

# ── Step 4: Deploy agents ──────────────────────────────────────────────────────
echo ""
echo "==> Step 4: Deploy agents"

echo "--- asia-west agent on $MINIPC_HOST"
bash "$SCRIPTS/deploy-agent.sh" $MOCK_FLAG \
  --collector "$WEST_GRPC" --region asia-west "$MINIPC_HOST"

if [[ -n "$EAST_HTTP" ]]; then
  echo "--- asia-east agent on $YUKI_HOST"
  bash "$SCRIPTS/deploy-agent.sh" $MOCK_FLAG \
    --collector "$EAST_GRPC" --region asia-east "$YUKI_HOST"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "==> Multi-region demo deployed!"
echo ""
echo "  Region collectors:"
echo "    asia-west  — ${WEST_HTTP}/api/nodes"
if [[ -n "$EAST_HTTP" ]]; then
  echo "    asia-east  — ${EAST_HTTP}/api/nodes"
fi
echo ""
echo "  Aggregator (merged view):"
echo "    ${AGG_HTTP}/api/nodes"
echo "    ${AGG_HTTP}/api/regions"
echo ""
echo "  Quick check:"
echo "    curl -s ${AGG_HTTP}/api/regions | jq ."
echo "    curl -s ${AGG_HTTP}/api/nodes   | jq '.[] | {node_id, region, status}'"
