#!/usr/bin/env bash
# Regression test for #580: the generated agent unit must carry hardening
# directives and keep the capabilities eBPF needs, without directives that
# could break eBPF (not verified on real hardware). ssh/scp/sudo are stubbed;
# the unit written via `sudo tee` is captured. Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist" "$TMP/stubs"
cp "$SRC/scripts/deploy-agent.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
: > "$TMP/dist/sentinel-agent"

# ssh stub: capture the unit when it is piped to tee; swallow other stdin.
cat > "$TMP/stubs/ssh" <<STUB
#!/bin/sh
case "\$*" in
  *mktemp*) echo "/tmp/tmp.RANDOM1234"; exit 0 ;;
  *hostname*) echo "192.0.2.1"; exit 0 ;;
  *tee*) cat > "$TMP/unit" ;;
  *"bash -s"*|*"/dev/stdin"*) cat >/dev/null ;;
esac
exit 0
STUB
for cmd in scp sudo; do printf '#!/bin/sh\nexit 0\n' > "$TMP/stubs/$cmd"; done
chmod +x "$TMP/stubs/"*
export PATH="$TMP/stubs:$PATH"

fail=0
has()   { if grep -qxF -- "$3" "$2" 2>/dev/null; then echo "ok   $1"; else echo "FAIL $1 (missing line: $3)"; fail=1; fi; }
hasnt() { if grep -qE -- "$3" "$2" 2>/dev/null; then echo "FAIL $1 (found: $3)"; fail=1; else echo "ok   $1"; fi; }

rm -f "$TMP/unit"
bash "$TMP/scripts/deploy-agent.sh" --collector 192.0.2.10:50051 testhost >/dev/null 2>&1 \
  || { echo "FAIL deploy exited non-zero"; fail=1; }
has   "NoNewPrivileges"        "$TMP/unit" "NoNewPrivileges=yes"
has   "ProtectSystem=strict"   "$TMP/unit" "ProtectSystem=strict"
has   "ProtectHome=yes"        "$TMP/unit" "ProtectHome=yes"
has   "PrivateTmp"             "$TMP/unit" "PrivateTmp=yes"
has   "ProtectControlGroups"   "$TMP/unit" "ProtectControlGroups=yes"
has   "CapabilityBoundingSet"  "$TMP/unit" "CapabilityBoundingSet=CAP_BPF CAP_PERFMON CAP_SYS_PTRACE"
hasnt "no AmbientCapabilities" "$TMP/unit" '^AmbientCapabilities='
# Directives that could break eBPF must stay out (see comment in make_unit).
for d in ProtectKernelTunables ProtectKernelModules ProtectKernelLogs PrivateDevices \
         MemoryDenyWriteExecute SystemCallFilter RestrictAddressFamilies; do
  hasnt "no $d" "$TMP/unit" "^$d="
done
# Existing directives are preserved.
has   "EnvironmentFile kept"   "$TMP/unit" "EnvironmentFile=-/etc/sentinel-agent-token.env"
has   "Restart kept"           "$TMP/unit" "Restart=on-failure"

# CA cert under a home-like dir: ProtectHome=yes would hide it -> read-only.
for ca in /home/u/ca.pem /root/ca.pem /run/user/1000/ca.pem; do
  rm -f "$TMP/unit"
  bash "$TMP/scripts/deploy-agent.sh" --collector 192.0.2.10:50051 --grpc-ca-cert "$ca" testhost >/dev/null 2>&1 \
    || { echo "FAIL deploy exited non-zero for $ca"; fail=1; }
  has "ProtectHome=read-only for $ca" "$TMP/unit" "ProtectHome=read-only"
done
rm -f "$TMP/unit"
bash "$TMP/scripts/deploy-agent.sh" --collector 192.0.2.10:50051 --grpc-ca-cert /etc/sentinel/ca.pem testhost >/dev/null 2>&1
has "ProtectHome=yes for /etc CA" "$TMP/unit" "ProtectHome=yes"

# Mock mode gets the same hardening.
rm -f "$TMP/unit"
bash "$TMP/scripts/deploy-agent.sh" --mock --collector 192.0.2.10:50051 testhost >/dev/null 2>&1
has "mock: NoNewPrivileges" "$TMP/unit" "NoNewPrivileges=yes"

if command -v systemd-analyze >/dev/null 2>&1 && [[ -s "$TMP/unit" ]]; then
  # ExecStart binary is absent locally; only flag syntax/directive errors.
  cp "$TMP/unit" "$TMP/sentinel-agent.service"
  va="$(LC_ALL=C systemd-analyze verify "$TMP/sentinel-agent.service" 2>&1 | grep -iE 'unknown (key|section)|Unknown lvalue|Invalid|Failed to parse' || true)"
  if [[ -n "$va" ]]; then echo "FAIL systemd-analyze: $va"; fail=1; else echo "ok   systemd-analyze verify"; fi
fi

exit "$fail"
