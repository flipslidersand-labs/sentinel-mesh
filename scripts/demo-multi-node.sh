#!/usr/bin/env bash
# One-shot: build + deploy collector to MINIPC + deploy agents to available nodes.
# Skips unreachable nodes automatically.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$REPO_ROOT/scripts"

COLLECTOR_HOST="minipc"
COLLECTOR_IP="192.168.68.63"
COLLECTOR_ADDR="$COLLECTOR_IP:50051"

declare -A AGENTS=(
  ["minipc"]="minipc"          # MINIPC self-agent for local events
  ["yuki-private"]="yuki"      # YUKI-PRIVATE002
  ["ds1"]="ds1"                # DS1HANAHANA (skipped if offline)
)

echo "==> Step 1: Build binaries"
bash "$SCRIPTS/build.sh"

echo ""
echo "==> Step 2: Deploy Collector to $COLLECTOR_HOST ($COLLECTOR_IP)"
bash "$SCRIPTS/install-collector.sh" "$COLLECTOR_HOST"

echo ""
echo "==> Step 3: Deploy Agents"
for ssh_host in "${!AGENTS[@]}"; do
  node_id="${AGENTS[$ssh_host]}"
  if ssh -o ConnectTimeout=5 -o BatchMode=yes "$ssh_host" true 2>/dev/null; then
    echo "--- Deploying agent '$node_id' to $ssh_host (mock mode)"
    bash "$SCRIPTS/deploy-agent.sh" --mock --collector "$COLLECTOR_ADDR" --node-id "$node_id" "$ssh_host"
  else
    echo "--- Skipping $ssh_host (unreachable)"
  fi
done

echo ""
echo "==> Done! Dashboard: http://$COLLECTOR_IP:8081"
echo "    Check nodes: curl http://$COLLECTOR_IP:8081/api/nodes"
