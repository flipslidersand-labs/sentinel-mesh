#!/usr/bin/env bash
# Regression test for #420: --grpc-token without --tls to a non-loopback
# collector must be refused before any ssh/scp/sudo (the agent itself refuses
# this at start, see check_token_transport in rust-agent/agent/src/main.rs).
# Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist" "$TMP/stubs"
cp "$SRC/scripts/deploy-agent.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
printf 'xx\0mode: eBPF\0yy' > "$TMP/dist/sentinel-agent"
for cmd in ssh scp sudo; do
  printf '#!/bin/sh\necho "%s $*" >> "%s/stub-calls"\nexit 99\n' "$cmd" "$TMP" > "$TMP/stubs/$cmd"
  chmod +x "$TMP/stubs/$cmd"
done
export PATH="$TMP/stubs:$PATH"

fail=0
# expect_refused|expect_ok NAME ARGS...  (collector given in ARGS)
check() {
  local want="$1" name="$2"; shift 2
  : > "$TMP/stub-calls"
  local out rc
  out="$(bash "$TMP/scripts/deploy-agent.sh" --node-id web-01 "$@" testhost 2>&1)"; rc=$?
  if [[ "$want" == refused ]]; then
    if [[ $rc -ne 0 && "$out" == *"plaintext"* && ! -s "$TMP/stub-calls" ]]; then
      echo "ok   $name refused"
    else
      echo "FAIL $name not refused (rc=$rc): $out"; fail=1
    fi
  else
    if [[ "$out" != *"plaintext"* && -s "$TMP/stub-calls" ]]; then
      echo "ok   $name proceeds"
    else
      echo "FAIL $name was refused: $out"; fail=1
    fi
  fi
}

check refused "token, no tls, remote"        --grpc-token test-token --collector 192.0.2.10:50051
check refused "token, ca-cert only, remote"  --grpc-token test-token --grpc-ca-cert /etc/ca.pem --collector 192.0.2.10:50051
check refused "token, fake loopback prefix"  --grpc-token test-token --collector 127.0.0.1.example.com:50051
check refused "token, out-of-range 127.x"    --grpc-token test-token --collector 127.0.0.300:50051
check ok      "token + tls, remote"          --tls --grpc-token test-token --collector 192.0.2.10:50051
check ok      "token, loopback 127.0.0.1"    --grpc-token test-token --collector 127.0.0.1:50051
check ok      "token, loopback 127.5.6.7"    --grpc-token test-token --collector 127.5.6.7:50051
check ok      "token, localhost"             --grpc-token test-token --collector localhost:50051
check ok      "no token, no tls, remote"     --collector 192.0.2.10:50051
exit "$fail"
