#!/usr/bin/env bash
# Deploy and start sentinel-collector on a remote host via systemd.
#
# Usage:
#   install-collector.sh <SSH_HOST> [OPTIONS]
#
# Options:
#   --region REGION       region name this collector belongs to (default: "default")
#   --grpc-addr ADDR      gRPC listen address (default: :50051)
#   --http-addr ADDR      HTTP listen address (default: :8081)
#   --data-dir DIR        BadgerDB data directory (default: /var/lib/sentinel)
#   --grpc-tls-cert PATH  path (on SSH_HOST) to gRPC TLS certificate (PEM); requires --grpc-tls-key
#   --grpc-tls-key PATH   path (on SSH_HOST) to gRPC TLS private key (PEM); requires --grpc-tls-cert
#
#   --grpc-auth MODE      gRPC auth: required (default) or disabled (#222)
#   --http-auth MODE      REST auth: required (default) or disabled (#254)
#
# gRPC auth: the default (required) rejects every RPC until per-agent tokens
# are issued on the collector (`sentinel-collector token issue <node_id>`;
# stop the service first, BadgerDB is exclusively locked). Pass
# `--grpc-auth disabled` only for dev/trusted networks. SENTINEL_API_TOKEN
# only gates the REST API and no longer affects gRPC auth.
#
# REST auth: the default (required) makes the collector refuse to start unless
# SENTINEL_API_TOKEN is set in its environment (e.g. a systemd drop-in with
# EnvironmentFile=, mode 600 — this script never writes the token). Pass
# `--http-auth disabled` only for dev/trusted networks. The script checks
# `systemctl is-active` after start and fails (with the journal tail) otherwise.
set -euo pipefail

SSH_HOST="${1:?Usage: $0 <SSH_HOST> [options]}"
shift

REGION="default"
GRPC_ADDR=":50051"
HTTP_ADDR=":8081"
DATA_DIR="/var/lib/sentinel"
GRPC_TLS_CERT=""
GRPC_TLS_KEY=""
GRPC_AUTH="required"
HTTP_AUTH="required"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)       REGION="$2";       shift 2 ;;
    --grpc-addr)    GRPC_ADDR="$2";    shift 2 ;;
    --http-addr)    HTTP_ADDR="$2";    shift 2 ;;
    --data-dir)     DATA_DIR="$2";     shift 2 ;;
    --grpc-tls-cert) GRPC_TLS_CERT="$2"; shift 2 ;;
    --grpc-tls-key)  GRPC_TLS_KEY="$2";  shift 2 ;;
    --grpc-auth)    GRPC_AUTH="$2";    shift 2 ;;
    --http-auth)    HTTP_AUTH="$2";    shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib-validate.sh"

if [[ "$GRPC_AUTH" != "required" && "$GRPC_AUTH" != "disabled" ]]; then
  echo "Error: --grpc-auth must be 'required' or 'disabled', got '${GRPC_AUTH}'" >&2
  exit 1
fi

if [[ "$HTTP_AUTH" != "required" && "$HTTP_AUTH" != "disabled" ]]; then
  echo "Error: --http-auth must be 'required' or 'disabled', got '${HTTP_AUTH}'" >&2
  exit 1
fi

# A leading '-' in SSH_HOST would be interpreted by ssh/scp as an option
# (e.g. -oProxyCommand=...), letting it run arbitrary local commands (#66).
validate_host "SSH_HOST" "$SSH_HOST"

# Every value below is interpolated into the systemd unit written below via
# `sudo tee` — validate before use (#65).
validate_ident "--region" "$REGION"
validate_listen_addr "--grpc-addr" "$GRPC_ADDR"
validate_listen_addr "--http-addr" "$HTTP_ADDR"
validate_abs_path "--data-dir" "$DATA_DIR"
[[ -n "$GRPC_TLS_CERT" ]] && validate_path "--grpc-tls-cert" "$GRPC_TLS_CERT"
[[ -n "$GRPC_TLS_KEY" ]] && validate_path "--grpc-tls-key" "$GRPC_TLS_KEY"

TLS_FLAGS=""
if [[ -n "$GRPC_TLS_CERT" && -n "$GRPC_TLS_KEY" ]]; then
  TLS_FLAGS="--grpc-tls-cert ${GRPC_TLS_CERT} --grpc-tls-key ${GRPC_TLS_KEY}"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$REPO_ROOT/dist"
BINARY="$DIST/sentinel-collector"
REMOTE_BIN="/usr/local/bin/sentinel-collector"
REMOTE_STATIC="/opt/sentinel-collector/static"
STATIC_DIR="$REPO_ROOT/go-collector/static"

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found. Run scripts/build.sh first."
  exit 1
fi

REMOTE_TMP_BIN="/tmp/sentinel-collector"
REMOTE_TMP_STATIC="/tmp/sentinel-static"

# Remote commands are sent as positional args to `bash -s --` inside a
# single-quoted heredoc, so the remote shell never re-parses an interpolated
# value as command text (#66). ssh/scp get a `--` guard so a (validated,
# non-'-'-leading) host can never be misread as an option either.
echo "==> Deploying collector to $SSH_HOST (region=${REGION})..."
ssh -- "$SSH_HOST" bash -s -- "$REMOTE_STATIC" <<'REMOTE'
set -euo pipefail
sudo mkdir -p "$1"
REMOTE

scp -- "$BINARY" "$SSH_HOST:$REMOTE_TMP_BIN"
ssh -- "$SSH_HOST" bash -s -- "$REMOTE_TMP_BIN" "$REMOTE_BIN" <<'REMOTE'
set -euo pipefail
sudo mv "$1" "$2"
sudo chmod +x "$2"
REMOTE

if [[ -d "$STATIC_DIR" ]]; then
  echo "==> Uploading UI static files..."
  scp -r -- "$STATIC_DIR/." "$SSH_HOST:$REMOTE_TMP_STATIC/"
  ssh -- "$SSH_HOST" bash -s -- "$REMOTE_TMP_STATIC" "$REMOTE_STATIC" <<'REMOTE'
set -euo pipefail
sudo rsync -a "$1/" "$2/"
rm -rf "$1"
REMOTE
fi

echo "==> Installing systemd service..."
ssh -- "$SSH_HOST" "sudo tee /etc/systemd/system/sentinel-collector.service > /dev/null" <<EOF
[Unit]
Description=SentinelMesh Collector (region=${REGION})
After=network.target

[Service]
ExecStart=${REMOTE_BIN} serve --grpc-addr ${GRPC_ADDR} --http-addr ${HTTP_ADDR} --data-dir ${DATA_DIR} --static-dir ${REMOTE_STATIC} --region ${REGION} --grpc-auth ${GRPC_AUTH} --http-auth ${HTTP_AUTH} ${TLS_FLAGS}
WorkingDirectory=/opt/sentinel-collector
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

ssh -- "$SSH_HOST" bash -s <<'REMOTE'
set -euo pipefail
sudo systemctl daemon-reload
sudo systemctl enable --now sentinel-collector
sleep 3
if ! systemctl is-active --quiet sentinel-collector; then
  echo "Error: sentinel-collector failed to start; recent log:" >&2
  sudo journalctl -u sentinel-collector -n 15 --no-pager >&2 || true
  exit 1
fi
REMOTE

echo "==> sentinel-collector running on $SSH_HOST ${GRPC_ADDR} (gRPC) ${HTTP_ADDR} (HTTP)"
DASH_IP="$(ssh -- "$SSH_HOST" hostname -I)"
DASH_IP="${DASH_IP%% *}"
echo "    Dashboard: http://${DASH_IP}${HTTP_ADDR}"
