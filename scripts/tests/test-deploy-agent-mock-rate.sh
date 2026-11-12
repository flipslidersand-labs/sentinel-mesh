#!/usr/bin/env bash
# Regression test for #260 item 2: --mock-rate must be an integer in 1..1000.
# Runs a copy of deploy-agent.sh against a dummy binary; invalid values must
# exit non-zero before any ssh/sudo is reached. Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist"
cp "$SRC/scripts/deploy-agent.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
: > "$TMP/dist/sentinel-agent"

# Stub ssh/scp/sudo first on PATH: if validation regresses and the script
# reaches them, the stub records the call and the test fails.
mkdir -p "$TMP/stubs"
for cmd in ssh scp sudo; do
  printf '#!/bin/sh\necho "%s $*" >> "%s/stub-calls"\nexit 99\n' "$cmd" "$TMP" > "$TMP/stubs/$cmd"
  chmod +x "$TMP/stubs/$cmd"
done
export PATH="$TMP/stubs:$PATH"

fail=0
expect_reject() {
  local label="$1" value="$2" out rc
  out="$(bash "$TMP/scripts/deploy-agent.sh" --mock --mock-rate "$value" --collector 192.0.2.10:50051 testhost 2>&1)"
  rc=$?
  if [[ $rc -ne 0 && "$out" == *"--mock-rate must be an integer between 1 and 1000"* ]]; then
    echo "ok   reject $label"
  else
    echo "FAIL reject $label (rc=$rc): $out"; fail=1
  fi
}

expect_reject "empty"            ""
expect_reject "non-numeric"      "abc"
expect_reject "negative"         "-5"
expect_reject "zero"             "0"
expect_reject "1001"             "1001"
expect_reject "newline injected" $'3\n[Service]\nExecStartPre=/bin/false'
expect_reject "trailing newline" $'3\n'
expect_reject "overflow"         "18446744073709551617"

# Accepted values must pass validation (checked via the lib, no ssh reached).
# shellcheck source=scripts/lib-validate.sh
# shellcheck source-path=SCRIPTDIR/../..
source "$SRC/scripts/lib-validate.sh"
for v in 1 3 1000; do
  if (validate_mock_rate "--mock-rate" "$v") >/dev/null 2>&1; then
    echo "ok   accept $v"
  else
    echo "FAIL accept $v"; fail=1
  fi
done

if [[ -s "$TMP/stub-calls" ]]; then
  echo "FAIL ssh/scp/sudo was reached:"; cat "$TMP/stub-calls"; fail=1
fi

exit "$fail"
