#!/usr/bin/env bash
# Build collector (Go) and agent (Rust) for Linux/amd64
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$REPO_ROOT/dist"
mkdir -p "$DIST"

echo "==> Building Go Collector (linux/amd64)..."
cd "$REPO_ROOT/go-collector"
GOOS=linux GOARCH=amd64 go build -o "$DIST/sentinel-collector" ./cmd/collector
echo "    $DIST/sentinel-collector"

echo "==> Building Rust Agent (linux/amd64, mock mode)..."
cd "$REPO_ROOT/rust-agent/agent"
cargo build --release --target x86_64-unknown-linux-gnu -q
cp "$REPO_ROOT/rust-agent/target/x86_64-unknown-linux-gnu/release/sentinel-agent" "$DIST/sentinel-agent"
echo "    $DIST/sentinel-agent"

echo ""
echo "Done. Binaries in $DIST/"
ls -lh "$DIST/"
