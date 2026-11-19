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
#
# The service runs as the unprivileged system user `sentinel` with systemd
# hardening (#365). Re-running on an older root-based install migrates it: the
# data dir is chowned to `sentinel`. TLS cert/key must be readable by that user.
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

# The installer runs `chown -R` / `chmod 700` on --data-dir: refuse paths that
# are, or sit directly in, a system directory (#365). Needs >= 2 components
# (/var/lib/<name> ok, /var/lib not), no '.'/'..' parts, and not a known system dir.
data_norm="${DATA_DIR%"${DATA_DIR##*[!/]}"}"
if [[ "$data_norm" == */./* || "$data_norm" == */../* || "$data_norm" == */. || "$data_norm" == */.. || "$data_norm" == *//* ]]; then
  echo "Error: --data-dir must be a normalized path (no '.', '..' or '//'): '${DATA_DIR}'" >&2
  exit 1
fi
data_depth="$(awk -F/ '{n=0; for(i=1;i<=NF;i++) if($i!="") n++; print n}' <<<"$data_norm")"
case "$data_norm" in
  /usr|/usr/*|/etc|/etc/*|/bin|/sbin|/lib|/lib64|/boot|/dev|/proc|/sys|/root|/root/*|/home|/home/*|\
  /var/lib|/var/log|/var/run|/var/tmp|/var/cache|/var/spool|/var/mail|/run|/run/*|/opt|/srv|/tmp|/mnt|/media)
    if [[ "$data_norm" != /usr/local/* && "$data_norm" != /srv/* && "$data_norm" != /opt/* && "$data_norm" != /mnt/* && "$data_norm" != /media/* && "$data_norm" != /tmp/* ]] \
       || [[ "$data_depth" -lt 2 ]] || [[ "$data_norm" == /usr/local/bin* || "$data_norm" == /usr/local/sbin* || "$data_norm" == /usr/local/lib* ]]; then
      echo "Error: --data-dir '${DATA_DIR}' is a system directory; use a dedicated directory such as /var/lib/sentinel" >&2
      exit 1
    fi ;;
esac
if [[ "$data_depth" -lt 2 ]]; then
  echo "Error: --data-dir '${DATA_DIR}' is too shallow; use a dedicated directory such as /var/lib/sentinel" >&2
  exit 1
fi
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
SERVICE_USER="sentinel"
STATIC_DIR="$REPO_ROOT/go-collector/static"

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found. Run scripts/build.sh first."
  exit 1
fi

# Staging area on the target: a fresh random 0700 dir from `mktemp -d` on every
# run, never a fixed /tmp path another local user could pre-create or swap (#358).
# Removed on exit, including on failure.
REMOTE_TMP=""
cleanup_remote_tmp() {
  if [[ -n "$REMOTE_TMP" ]]; then
    ssh -n -- "$SSH_HOST" rm -rf -- "$REMOTE_TMP" >/dev/null 2>&1 || true
  fi
}
trap cleanup_remote_tmp EXIT
REMOTE_TMP="$(ssh -n -- "$SSH_HOST" mktemp -d)"
if [[ ! "$REMOTE_TMP" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
  echo "Error: unexpected remote mktemp output: '$REMOTE_TMP'" >&2
  REMOTE_TMP=""
  exit 1
fi
REMOTE_TMP_BIN="$REMOTE_TMP/sentinel-collector"
REMOTE_TMP_STATIC="$REMOTE_TMP/static"

# Remote commands are sent as positional args to `bash -s --` inside a
# single-quoted heredoc, so the remote shell never re-parses an interpolated
# value as command text (#66). ssh/scp get a `--` guard so a (validated,
# non-'-'-leading) host can never be misread as an option either.
echo "==> Deploying collector to $SSH_HOST (region=${REGION})..."
ssh -- "$SSH_HOST" bash -s -- "$REMOTE_STATIC" <<'REMOTE'
set -euo pipefail
sudo mkdir -p "$1"
REMOTE

# The collector does not need root (no eBPF): run it as a dedicated unprivileged
# user (#365). Create it, and hand the data dir to it (also migrates an existing
# root-owned data dir from an older install). TLS key/cert must already be
# readable by that user; we do not chown files we do not own (e.g. a shared
# certificate store) -- fail early with a hint instead.
echo "==> Preparing service user '${SERVICE_USER}' and ${DATA_DIR}..."
ssh -- "$SSH_HOST" bash -s -- "$SERVICE_USER" "$DATA_DIR" "$GRPC_TLS_CERT" "$GRPC_TLS_KEY" <<'REMOTE'
set -euo pipefail
user="$1"; data="$2"; cert="$3"; key="$4"
if ! id -u "$user" >/dev/null 2>&1; then
  sudo useradd --system --user-group --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin "$user"
fi
# An older root-based service may still hold the data dir: stop it before chown.
if systemctl is-active --quiet sentinel-collector; then
  sudo systemctl stop sentinel-collector
fi
sudo mkdir -p "$data"
sudo chown -R "$user": "$data"
sudo chmod 700 "$data"
for f in "$cert" "$key"; do
  [[ -z "$f" ]] && continue
  if ! sudo -u "$user" test -r "$f"; then
    echo "Error: $f is not readable by user '$user' (note ProtectHome=yes hides /home)." >&2
    echo "  Grant read access, e.g. a group ACL: sudo setfacl -m u:$user:r '$f' (and x on parent dirs)," >&2
    echo "  or copy the TLS files under /etc/sentinel-collector/ owned by '$user'." >&2
    exit 1
  fi
done
REMOTE

scp -- "$BINARY" "$SSH_HOST:$REMOTE_TMP_BIN"
ssh -- "$SSH_HOST" bash -s -- "$REMOTE_TMP_BIN" "$REMOTE_BIN" <<'REMOTE'
set -euo pipefail
sudo install -o root -g root -m 755 "$1" "$2"
rm -f "$1"
REMOTE

if [[ -d "$STATIC_DIR" ]]; then
  echo "==> Uploading UI static files..."
  ssh -n -- "$SSH_HOST" mkdir -p -- "$REMOTE_TMP_STATIC"
  scp -r -- "$STATIC_DIR/." "$SSH_HOST:$REMOTE_TMP_STATIC/"
  ssh -- "$SSH_HOST" bash -s -- "$REMOTE_TMP_STATIC" "$REMOTE_STATIC" <<'REMOTE'
set -euo pipefail
# No symlinks; never keep the deploy user's uid/gid. `--chown` needs rsync
# >= 3.1, so use it only when available and otherwise chown afterwards.
rsync_help="$(rsync --help 2>&1 || true)"
if [[ "$rsync_help" == *--chown* ]]; then
  sudo rsync -rt --no-links --chown=root:root --chmod=D755,F644 "$1/" "$2/"
else
  sudo rsync -rt --no-links --no-owner --no-group --chmod=D755,F644 "$1/" "$2/"
  sudo chown -R root:root "$2"
fi
rm -rf "$1"
REMOTE
fi

echo "==> Installing systemd service..."
ssh -- "$SSH_HOST" "sudo tee /etc/systemd/system/sentinel-collector.service > /dev/null" <<EOF
[Unit]
Description=SentinelMesh Collector (region=${REGION})
After=network.target

[Service]
User=${SERVICE_USER}
Group=${SERVICE_USER}
ExecStart=${REMOTE_BIN} serve --grpc-addr ${GRPC_ADDR} --http-addr ${HTTP_ADDR} --data-dir ${DATA_DIR} --static-dir ${REMOTE_STATIC} --region ${REGION} --grpc-auth ${GRPC_AUTH} --http-auth ${HTTP_AUTH} ${TLS_FLAGS}
WorkingDirectory=/opt/sentinel-collector
Restart=always
RestartSec=5
# Hardening (#365). Data dir is the only writable path.
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
LockPersonality=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
ReadWritePaths=${DATA_DIR}
# Allow binding ports below 1024 (e.g. --http-addr :80) without root.
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
EOF

ssh -- "$SSH_HOST" bash -s <<'REMOTE'
set -euo pipefail
sudo systemctl daemon-reload
sudo systemctl enable sentinel-collector
sudo systemctl restart sentinel-collector
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
