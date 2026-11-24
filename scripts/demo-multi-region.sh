#!/usr/bin/env bash
# Multi-region demo: two region collectors + a cross-region aggregator.
#
# Topology:
#   node-a  — region collector "asia-west"  :50051 gRPC  :8081 HTTP
#   node-b  — region collector "asia-east"  :50052 gRPC  :8082 HTTP
#   node-a  — aggregator (polls both)       :8083 HTTP   (no gRPC)
#
# Each region collector receives agents in its own region.
# The aggregator merges /api/nodes,events,alerts,regions from both collectors.
#
# Usage:
#   ./scripts/demo-multi-region.sh [--mock]
#
# NOTE: the collectors and aggregator run with auth disabled and listen on all
# interfaces (the aggregator polls the collectors across hosts, so loopback-only
# is not possible). Use only on an isolated demo network, and remove afterwards:
#   sudo systemctl disable --now sentinel-aggregator sentinel-collector
# SENTINEL_NODE_A_IP / SENTINEL_NODE_B_IP must be IPv4 addresses (#367).
#
# Requirements:
#   - scripts/build.sh already run
#   - SSH key access to node-a and node-b
#   - sudo on both hosts
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$REPO_ROOT/scripts"
# shellcheck source=scripts/lib-validate.sh
source "$SCRIPTS/lib-validate.sh"

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
NODE_A_HOST="node-a"
NODE_A_IP="${SENTINEL_NODE_A_IP:-192.0.2.10}"
NODE_B_HOST="node-b"
NODE_B_IP="${SENTINEL_NODE_B_IP:-192.0.2.11}"

# These values are interpolated into a root systemd unit and ssh arguments:
# validate before any use (#65, #367). Fail closed.
validate_host "NODE_A_HOST" "$NODE_A_HOST"
validate_host "NODE_B_HOST" "$NODE_B_HOST"
validate_ip "SENTINEL_NODE_A_IP" "$NODE_A_IP"
validate_ip "SENTINEL_NODE_B_IP" "$NODE_B_IP"

WEST_GRPC="${NODE_A_IP}:50051"
WEST_HTTP="http://${NODE_A_IP}:8081"
EAST_GRPC="${NODE_B_IP}:50052"
EAST_HTTP="http://${NODE_B_IP}:8082"
AGG_HTTP="http://${NODE_A_IP}:8083"

# ── Step 1: build ──────────────────────────────────────────────────────────────
echo "==> Step 1: Build binaries"
bash "$SCRIPTS/build.sh"

# ── Step 2: Deploy region collectors ──────────────────────────────────────────
echo ""
echo "==> Step 2a: Deploy asia-west collector → $NODE_A_HOST"
bash "$SCRIPTS/install-collector.sh" "$NODE_A_HOST" \
  --region asia-west --grpc-addr :50051 --http-addr :8081 --grpc-auth disabled --http-auth disabled

if ssh -o ConnectTimeout=5 -o BatchMode=yes -- "$NODE_B_HOST" true 2>/dev/null; then
  echo ""
  echo "==> Step 2b: Deploy asia-east collector → $NODE_B_HOST"
  bash "$SCRIPTS/install-collector.sh" "$NODE_B_HOST" \
    --region asia-east --grpc-addr :50052 --http-addr :8082 --grpc-auth disabled --http-auth disabled
else
  echo "==> Step 2b: $NODE_B_HOST unreachable — skipping asia-east collector"
  EAST_HTTP=""
fi

# ── Step 3: Deploy aggregator on node-a ───────────────────────────────────────
echo ""
echo "==> Step 3: Deploy aggregator → $NODE_A_HOST :8083"

# Demo only: upstreams are plaintext http:// on non-loopback hosts, so the
# aggregator needs --aggregate-allow-insecure-http (#359); use TLS in production.
UPSTREAMS="asia-west=${WEST_HTTP}"
if [[ -n "$EAST_HTTP" ]]; then
  UPSTREAMS="${UPSTREAMS},asia-east=${EAST_HTTP}"
fi

# Client-side expansion is intended; every value is validated above.
# shellcheck disable=SC2087
ssh -- "$NODE_A_HOST" "sudo tee /etc/systemd/system/sentinel-aggregator.service > /dev/null" <<EOF
[Unit]
Description=SentinelMesh Aggregator
After=network.target sentinel-collector.service

[Service]
ExecStart=/usr/local/bin/sentinel-collector serve --aggregate --aggregate-allow-insecure-http --http-auth disabled --http-addr :8083 --upstreams ${UPSTREAMS} --poll-interval 10s
WorkingDirectory=/opt/sentinel-collector
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
ssh -- "$NODE_A_HOST" "sudo systemctl daemon-reload && sudo systemctl enable --now sentinel-aggregator"

# ── Step 4: Deploy agents ──────────────────────────────────────────────────────
echo ""
echo "==> Step 4: Deploy agents"

echo "--- asia-west agent on $NODE_A_HOST"
bash "$SCRIPTS/deploy-agent.sh" $MOCK_FLAG \
  --collector "$WEST_GRPC" --region asia-west "$NODE_A_HOST"

if [[ -n "$EAST_HTTP" ]]; then
  echo "--- asia-east agent on $NODE_B_HOST"
  bash "$SCRIPTS/deploy-agent.sh" $MOCK_FLAG \
    --collector "$EAST_GRPC" --region asia-east "$NODE_B_HOST"
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
