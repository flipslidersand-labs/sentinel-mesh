#!/bin/bash
set -eo pipefail

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
  --grpc-auth disabled \
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

# 失敗時は該当レスポンスを表示して終了する
fail() {
  echo "❌ FAIL: $1"
  if [ -n "${2:-}" ]; then
    echo "--- response ---"
    echo "$2"
    echo "----------------"
  fi
  exit 1
}

# wait_until <timeout秒> <説明> <コマンド...>: 条件成立まで1秒間隔でポーリング。タイムアウトで fail
wait_until() {
  local timeout="$1" desc="$2"
  shift 2
  local i
  for ((i = 0; i < timeout; i++)); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  fail "${desc} (${timeout}秒以内に条件が成立しませんでした)"
}

has_events() {
  curl -fsS "$BASE_URL/api/events?limit=3" | jq -e 'length >= 1'
}

has_sentinel_metrics() {
  local body
  body=$(curl -fsS "$BASE_URL/metrics") || return 1
  grep -q "^sentinel_" <<<"$body"
}

# Health check
echo ""
echo "🏥 Health Check:"
HEALTH_CODE=$(curl -sS -o /tmp/healthz.body -w '%{http_code}' "$BASE_URL/healthz") \
  || fail "/healthz に接続できません"
[ "$HEALTH_CODE" = "200" ] || fail "/healthz が 200 ではありません (got $HEALTH_CODE)" "$(cat /tmp/healthz.body)"
cat /tmp/healthz.body | jq .
rm -f /tmp/healthz.body

# Event stats (情報表示のみ)
echo ""
echo "📈 Event Stats:"
curl -fsS "$BASE_URL/api/stats" | jq .

# Node status: 1件以上かつ全ノード active
echo ""
echo "🖥️  Registered Nodes:"
NODES=$(curl -fsS "$BASE_URL/api/nodes") || fail "/api/nodes の取得に失敗しました"
echo "$NODES" | jq '.[] | {node_id, hostname, status}'
echo "$NODES" | jq -e 'length >= 1 and all(.[]; .status == "active")' >/dev/null \
  || fail "/api/nodes: ノードが0件、または active でないノードがあります" "$NODES"

# Recent events: 1件以上(流れ始めるまで最大15秒待つ)
echo ""
echo "📋 Recent Events (limit 3):"
wait_until 15 "/api/events にイベントがありません" has_events
EVENTS=$(curl -fsS "$BASE_URL/api/events?limit=3") || fail "/api/events の取得に失敗しました"
echo "$EVENTS" | jq '.[] | {type, node_id, timestamp}'

# Alerts: mock の発火内容は決定的でないため警告のみ(失敗にはしない)
echo ""
echo "🚨 Triggered Alerts:"
ALERTS=$(curl -fsS "$BASE_URL/api/alerts?limit=10") || fail "/api/alerts の取得に失敗しました"
ALERT_COUNT=$(echo "$ALERTS" | jq 'length') || fail "/api/alerts のレスポンスが不正です" "$ALERTS"
if [ "$ALERT_COUNT" -gt 0 ]; then
  echo "Found $ALERT_COUNT alerts:"
  echo "$ALERTS" | jq '.[] | {rule_id, severity, message, node_id}'
else
  echo "⚠️  WARNING: No alerts triggered (rules may need adjustment)"
fi

# Prometheus metrics: sentinel_ 系メトリクスが存在する
echo ""
echo "📊 Prometheus Metrics (sample):"
wait_until 15 "/metrics に sentinel_ 系メトリクスがありません" has_sentinel_metrics
METRICS=$(curl -fsS "$BASE_URL/metrics") || fail "/metrics の取得に失敗しました"
grep "^sentinel_" <<<"$METRICS" | head -10 || true

echo ""
echo "════════════════════════════════════════════════"
echo "✅ Integration test completed successfully!"
echo "════════════════════════════════════════════════"
echo ""
echo "📝 Log files:"
echo "  - Collector: /tmp/collector.log"
echo "  - Agent:     /tmp/agent.log"
echo ""
