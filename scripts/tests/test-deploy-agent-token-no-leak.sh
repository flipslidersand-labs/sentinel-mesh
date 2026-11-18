#!/usr/bin/env bash
# Regression test for #363: an invalid --grpc-token must be rejected without
# echoing the token value to stderr/stdout (CI logs, terminal history).
# Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist" "$TMP/stubs"
cp "$SRC/scripts/deploy-agent.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
: > "$TMP/dist/sentinel-agent"
for cmd in ssh scp sudo; do
  printf '#!/bin/sh\necho "%s $*" >> "%s/stub-calls"\nexit 99\n' "$cmd" "$TMP" > "$TMP/stubs/$cmd"
  chmod +x "$TMP/stubs/$cmd"
done
export PATH="$TMP/stubs:$PATH"

fail=0
# Obviously-dummy secret values containing a char outside [A-Za-z0-9._-].
for token in "DUMMY-TOKEN-NOT-REAL;x" 'DUMMY-TOKEN-NOT-REAL with space'; do
  out="$(bash "$TMP/scripts/deploy-agent.sh" --mock --node-id web-01 --grpc-token "$token" --collector 192.0.2.10:50051 testhost 2>&1)"
  rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "FAIL accepted invalid token: $out"; fail=1
  elif [[ "$out" == *"DUMMY-TOKEN"* ]]; then
    echo "FAIL token value leaked: $out"; fail=1
  elif [[ "$out" != *"--grpc-token"* ]]; then
    echo "FAIL field name missing from error: $out"; fail=1
  else
    echo "ok   reject without leaking value"
  fi
done
exit "$fail"
