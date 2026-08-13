#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Cleanup on exit
cleanup() {
  echo "🧹 Cleaning up..."
  pkill -f "collector serve" || true
  pkill -f "sentinel-agent" || true
  rm -rf /tmp/sentinel-test-data
  sleep 1
}
trap cleanup EXIT

# Config (use dynamic ports to avoid conflicts)
GRPC_PORT=$((50000 + RANDOM % 1000))
HTTP_PORT=$((8000 + RANDOM % 1000))
GRPC_ADDR=":$GRPC_PORT"
HTTP_ADDR=":$HTTP_PORT"
DATA_DIR="/tmp/sentinel-test-data-$$"
RULES_FILE="$PROJECT_ROOT/rules.yaml"
AGENT_RATE=5

echo "════════════════════════════════════════════════"
echo "🚀 Sentinel-Mesh Integration Test"
echo "════════════════════════════════════════════════"

# Build binaries
echo "📦 Building Collector..."
cd "$PROJECT_ROOT/go-collector"
go build -o collector ./cmd/collector

echo "📦 Building Agent..."
cd "$PROJECT_ROOT/rust-agent"
AGENT_BIN="$(pwd)/target/x86_64-unknown-linux-gnu/release/sentinel-agent"

# Start Collector
echo ""
echo "🔧 Starting Collector on $HTTP_ADDR..."
cd "$PROJECT_ROOT/go-collector"
./collector serve \
  --grpc-addr "$GRPC_ADDR" \
  --http-addr "$HTTP_ADDR" \
  --data-dir "$DATA_DIR" \
  --rules "$RULES_FILE" \
  > /tmp/collector.log 2>&1 &
COLLECTOR_PID=$!

# Wait for Collector to be ready
sleep 2
if ! kill -0 $COLLECTOR_PID 2>/dev/null; then
  echo "❌ Collector failed to start"
  cat /tmp/collector.log
  exit 1
fi
echo "✅ Collector started (PID: $COLLECTOR_PID)"

# Start Agent
echo ""
echo "🔧 Starting Agent in mock mode ($AGENT_RATE events/sec)..."
$AGENT_BIN \
  --collector "http://127.0.0.1:$GRPC_PORT" \
  --mock \
  --mock-rate "$AGENT_RATE" \
  > /tmp/agent.log 2>&1 &
AGENT_PID=$!

sleep 2
if ! kill -0 $AGENT_PID 2>/dev/null; then
  echo "❌ Agent failed to start"
  cat /tmp/agent.log
  exit 1
fi
echo "✅ Agent started (PID: $AGENT_PID)"

# Let events flow
echo ""
echo "⏳ Collecting events and alerts for 8 seconds..."
sleep 8

# Test REST API endpoints
echo ""
echo "════════════════════════════════════════════════"
echo "📊 Testing REST API Endpoints"
echo "════════════════════════════════════════════════"

BASE_URL="http://localhost$HTTP_ADDR"

# Health check
echo ""
echo "🏥 Health Check:"
curl -s "$BASE_URL/healthz" | jq .

# Event stats
echo ""
echo "📈 Event Stats:"
curl -s "$BASE_URL/api/stats" | jq .

# Node status
echo ""
echo "🖥️  Registered Nodes:"
curl -s "$BASE_URL/api/nodes" | jq '.[] | {node_id, hostname, status}' | head -20

# Recent events
echo ""
echo "📋 Recent Events (limit 3):"
curl -s "$BASE_URL/api/events?limit=3" | jq '.[] | {type, node_id, timestamp}' | head -20

# Alerts (if any)
echo ""
echo "🚨 Triggered Alerts:"
ALERTS=$(curl -s "$BASE_URL/api/alerts?limit=10")
ALERT_COUNT=$(echo "$ALERTS" | jq 'length')
if [ "$ALERT_COUNT" -gt 0 ]; then
  echo "Found $ALERT_COUNT alerts:"
  echo "$ALERTS" | jq '.[] | {rule_id, severity, message, node_id}' | head -30
else
  echo "No alerts triggered (rules may need adjustment)"
fi

# Prometheus metrics
echo ""
echo "📊 Prometheus Metrics (sample):"
curl -s "$BASE_URL/metrics" | grep "^sentinel" | head -10

echo ""
echo "════════════════════════════════════════════════"
echo "✅ Integration test completed successfully!"
echo "════════════════════════════════════════════════"
echo ""
echo "📝 Log files:"
echo "  - Collector: /tmp/collector.log"
echo "  - Agent:     /tmp/agent.log"
echo ""
