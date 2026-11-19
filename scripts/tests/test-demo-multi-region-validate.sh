#!/usr/bin/env bash
# Regression test for #367: demo-multi-region.sh must validate SENTINEL_*_IP
# before anything reaches ssh/sudo or a systemd unit. Touches only a tmp dir;
# build.sh/install-collector.sh/deploy-agent.sh are stubbed and ssh never runs.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts"
cp "$SRC/scripts/demo-multi-region.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
for s in build.sh install-collector.sh deploy-agent.sh; do
  printf '#!/bin/sh\necho "%s $*" >> "%s/stub-calls"\n' "$s" "$TMP" > "$TMP/scripts/$s"
done

mkdir -p "$TMP/stubs"
for cmd in ssh scp sudo; do
  printf '#!/bin/sh\necho "%s $*" >> "%s/stub-calls"\ncat >/dev/null 2>&1 </dev/null\nexit 99\n' "$cmd" "$TMP" > "$TMP/stubs/$cmd"
  chmod +x "$TMP/stubs/$cmd"
done
export PATH="$TMP/stubs:$PATH"

fail=0
expect_reject() {
  local label="$1" var="$2" value="$3" out rc
  rm -f "$TMP/stub-calls"
  out="$(env "$var=$value" bash "$TMP/scripts/demo-multi-region.sh" 2>&1 </dev/null)"
  rc=$?
  if [[ $rc -ne 0 && "$out" == *"must be an IPv4 address"* && ! -e "$TMP/stub-calls" ]]; then
    echo "ok   reject $var $label"
  else
    echo "FAIL reject $var $label (rc=$rc): $out"; fail=1
  fi
}

for var in SENTINEL_MINIPC_IP SENTINEL_YUKI_IP; do
  expect_reject "newline unit injection" "$var" $'192.0.2.10\n[Service]\nExecStartPre=/bin/false'
  expect_reject "shell metachar"         "$var" '192.0.2.10;id'
  expect_reject "option-like"            "$var" '-oProxyCommand=x'
  expect_reject "hostname"               "$var" 'example.com'
  expect_reject "octet > 255"            "$var" '192.0.2.256'
  expect_reject "trailing newline"       "$var" $'192.0.2.10\n'
done

# Valid defaults must reach ssh (stubbed) only after validation; ssh must use '--'.
rm -f "$TMP/stub-calls"
SENTINEL_MINIPC_IP=192.0.2.20 SENTINEL_YUKI_IP=192.0.2.21 \
  bash "$TMP/scripts/demo-multi-region.sh" >/dev/null 2>&1 </dev/null || true
if grep -q '^ssh ' "$TMP/stub-calls" && ! grep '^ssh ' "$TMP/stub-calls" | grep -qv -- ' -- '; then
  echo "ok   valid IPs reach ssh with --"
else
  echo "FAIL ssh calls missing '--':"; cat "$TMP/stub-calls" 2>/dev/null; fail=1
fi

# Demo upstreams are non-loopback http://, so the aggregator unit must opt in (#359).
if grep -q 'serve --aggregate --aggregate-allow-insecure-http ' "$TMP/scripts/demo-multi-region.sh"; then
  echo "ok   aggregator unit passes --aggregate-allow-insecure-http"
else
  echo "FAIL aggregator ExecStart lacks --aggregate-allow-insecure-http"; fail=1
fi

exit "$fail"
