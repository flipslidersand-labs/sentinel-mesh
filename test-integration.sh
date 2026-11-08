#!/bin/bash
set -eo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# 起動した PID と一時ファイルだけを後始末する(pkill -f は使わない)
COLLECTOR_PID=""
AGENT_PID=""
HEALTH_BODY=""
DATA_DIR="/tmp/sentinel-test-data-$$"

cleanup() {
  echo "🧹 Cleaning up..."
  local pid
  for pid in "$AGENT_PID" "$COLLECTOR_PID"; do
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$DATA_DIR"
  if [ -n "$HEALTH_BODY" ]; then
    rm -f "$HEALTH_BODY"
  fi
}
trap cleanup EXIT

# 空きポートを 2 つ取得する(同時に bind して重複を避ける)。標準ライブラリのみ使用
pick_free_ports() {
  python3 -c '
import socket
socks = []
for _ in range(2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    socks.append(s)
print(" ".join(str(s.getsockname()[1]) for s in socks))
'
}

# Config (空きポートを探索して衝突を避ける)
read -r GRPC_PORT HTTP_PORT < <(pick_free_ports)
if [ -z "$GRPC_PORT" ] || [ -z "$HTTP_PORT" ]; then
  echo "❌ 空きポートを取得できませんでした"
  exit 1
fi
GRPC_ADDR=":$GRPC_PORT"
HTTP_ADDR=":$HTTP_PORT"
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

BASE_URL="http://localhost$HTTP_ADDR"

# 失敗時は該当レスポンスを表示して終了する
fail() {
  echo "❌ FAIL: $1"
  if [ -n "${2:-}" ]; then
    echo "--- response ---"
    echo "$2"
    echo "----------------"
  fi
  for f in /tmp/collector.log /tmp/agent.log; do
    if [ -f "$f" ]; then
      echo "--- tail -50 $f ---"
      tail -50 "$f"
    fi
  done
  exit 1
}

# wait_until <timeout秒> <説明> <コマンド...>: 条件成立まで1秒間隔でポーリング。タイムアウトで fail
wait_until() {
  local timeout="$1" desc="$2"
  shift 2
  local i out=""
  for ((i = 0; i < timeout; i++)); do
    if out=$("$@" 2>&1); then
      return 0
    fi
    sleep 1
  done
  fail "${desc} (${timeout}秒以内に条件が成立しませんでした)" "$out"
}

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

# Wait for Collector to be ready (/healthz を最大30秒ポーリング)
collector_ready() {
  kill -0 "$COLLECTOR_PID" 2>/dev/null || { echo "collector process exited"; return 1; }
  curl -fsS "$BASE_URL/healthz"
}
wait_until 30 "Collector が起動しませんでした" collector_ready
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

# Agent: プロセス生存かつ Collector にノード登録されるまで最大30秒待つ
agent_registered() {
  kill -0 "$AGENT_PID" 2>/dev/null || { echo "agent process exited"; return 1; }
  curl -fsS "$BASE_URL/api/nodes" | jq -e 'length >= 1'
}
wait_until 30 "Agent が起動・登録されませんでした" agent_registered
echo "✅ Agent started (PID: $AGENT_PID)"

# Let events flow
echo ""
echo "⏳ Waiting for events to be collected..."

# Test REST API endpoints
echo ""
echo "════════════════════════════════════════════════"
echo "📊 Testing REST API Endpoints"
echo "════════════════════════════════════════════════"

has_min_stats() {
  curl -fsS "$BASE_URL/api/stats" | jq -e '([.[]] | add // 0) >= 10'
}

has_active_nodes() {
  curl -fsS "$BASE_URL/api/nodes" | jq -e 'length >= 1 and all(.[]; .status == "active")'
}

has_events() {
  curl -fsS "$BASE_URL/api/events?limit=3" | jq -e 'length >= 1'
}

has_events_metric() {
  local body
  body=$(curl -fsS "$BASE_URL/metrics") || return 1
  grep "^sentinel_events_total" <<<"$body" | awk '{ if ($NF + 0 > 0) found = 1 } END { exit !found }'
}

# Health check
echo ""
echo "🏥 Health Check:"
HEALTH_BODY=$(mktemp)
HEALTH_CODE=$(curl -sS -o "$HEALTH_BODY" -w '%{http_code}' "$BASE_URL/healthz") \
  || fail "/healthz に接続できません"
[ "$HEALTH_CODE" = "200" ] || fail "/healthz が 200 ではありません (got $HEALTH_CODE)" "$(cat "$HEALTH_BODY")"
jq . "$HEALTH_BODY"
rm -f "$HEALTH_BODY"

# Event stats: 合計 10 以上になるまで最大20秒待つ
echo ""
echo "📈 Event Stats:"
wait_until 20 "/api/stats のイベント合計が 10 未満です" has_min_stats
curl -fsS "$BASE_URL/api/stats" | jq .

# Node status: 1件以上かつ全ノード active
echo ""
echo "🖥️  Registered Nodes:"
wait_until 15 "/api/nodes: ノードが0件、または active でないノードがあります" has_active_nodes
curl -fsS "$BASE_URL/api/nodes" | jq '.[] | {node_id, hostname, status}'

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
wait_until 15 "/metrics に sentinel_events_total > 0 がありません" has_events_metric
METRICS=$(curl -fsS "$BASE_URL/metrics") || fail "/metrics の取得に失敗しました"
grep "^sentinel_events_total" <<<"$METRICS" | head -10 || true

echo ""
echo "════════════════════════════════════════════════"
echo "✅ Integration test completed successfully!"
echo "════════════════════════════════════════════════"
echo ""
echo "📝 Log files:"
echo "  - Collector: /tmp/collector.log"
echo "  - Agent:     /tmp/agent.log"
echo ""
