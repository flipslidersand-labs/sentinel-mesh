#!/usr/bin/env bash
# Regression test for #349: deploying a binary built without --features ebpf
# in non-mock mode must be refused before any ssh/scp/sudo. Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist" "$TMP/stubs"
cp "$SRC/scripts/deploy-agent.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
for cmd in ssh scp sudo; do
  printf '#!/bin/sh\necho "%s $*" >> "%s/stub-calls"\nexit 99\n' "$cmd" "$TMP" > "$TMP/stubs/$cmd"
  chmod +x "$TMP/stubs/$cmd"
done
export PATH="$TMP/stubs:$PATH"

fail=0
run() { bash "$TMP/scripts/deploy-agent.sh" "$@" --collector 192.0.2.10:50051 testhost 2>&1; }

# Binary contents mimic the strings of a no-eBPF build (message from main.rs).
printf 'xx\0error: compiled without --features ebpf; use --mock for testing\0yy' > "$TMP/dist/sentinel-agent"
out="$(run)"; rc=$?
if [[ $rc -ne 0 && "$out" == *"without eBPF support"* && "$out" == *"--mock"* && ! -s "$TMP/stub-calls" ]]; then
  echo "ok   non-mock + no-eBPF binary refused"
else
  echo "FAIL non-mock + no-eBPF binary not refused (rc=$rc): $out"; fail=1
fi

: > "$TMP/stub-calls"
out="$(run --mock)"
if [[ "$out" != *"without eBPF support"* && -s "$TMP/stub-calls" ]]; then
  echo "ok   --mock + no-eBPF binary proceeds to deploy"
else
  echo "FAIL --mock was refused: $out"; fail=1
fi

: > "$TMP/stub-calls"
printf 'xx\0mode: eBPF (requires CAP_BPF)\0yy' > "$TMP/dist/sentinel-agent"
out="$(run)"
if [[ "$out" != *"without eBPF support"* && -s "$TMP/stub-calls" ]]; then
  echo "ok   non-mock + eBPF binary proceeds to deploy"
else
  echo "FAIL eBPF binary was refused: $out"; fail=1
fi
exit "$fail"
