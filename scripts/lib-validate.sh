#!/usr/bin/env bash
# Shared input-validation helpers for the remote deploy scripts (#65).
#
# Every value below ends up interpolated into a systemd unit's ExecStart=
# line that is written to a root-owned file via `sudo tee`. An unvalidated
# value containing a newline or shell/systemd metacharacter lets an attacker
# inject arbitrary systemd directives (or, via a later ExecStart, arbitrary
# commands) into a root service. Validate everything before use — fail closed.

die() {
  echo "Error: $*" >&2
  exit 1
}

# Identifier-like values (node-id, region): letters/digits/._- only.
validate_ident() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[A-Za-z0-9._-]+$ ]] || die "$name must match ^[A-Za-z0-9._-]+\$: '$value'"
}

# Secret values (e.g. --grpc-token): same charset as validate_ident, but the
# error names only the field and never echoes the value, so a rejected token
# does not land in CI logs or terminal history (#363, follow-up to #174).
validate_secret() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[A-Za-z0-9._-]+$ ]] || die "$name must match ^[A-Za-z0-9._-]+\$ (value not shown)"
}

# SSH/SCP target host (or SSH config alias). Rejects a leading '-', which
# ssh/scp would otherwise interpret as an option (e.g. `-oProxyCommand=...`),
# letting an attacker-controlled hostname run arbitrary local commands (#66).
validate_host() {
  local name="$1" value="$2"
  [[ "$value" != -* ]] || die "$name must not start with '-': '$value'"
  [[ "$value" =~ ^[A-Za-z0-9._@-]+$ ]] || die "$name must match ^[A-Za-z0-9._@-]+\$: '$value'"
}

# IPv4 dotted quad, each octet 0..255 (no leading zeros beyond a single "0").
# Used for addresses interpolated into a systemd unit (e.g. --upstreams) (#367).
validate_ip() {
  local name="$1" value="$2" re='^(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})$'
  local i
  [[ "$value" =~ $re ]] || die "$name must be an IPv4 address: '$value'"
  for i in 1 2 3 4; do
    (( BASH_REMATCH[i] <= 255 )) || die "$name must be an IPv4 address: '$value'"
  done
}

# host:port pair with a required host (e.g. the collector address agents dial).
validate_hostport() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[A-Za-z0-9.-]+:[0-9]+$ ]] || die "$name must be host:port: '$value'"
}

# Listen address: ":port" or "host:port" (host optional, as gRPC/HTTP --*-addr
# flags default to ":<port>").
validate_listen_addr() {
  local name="$1" value="$2"
  [[ "$value" =~ ^([A-Za-z0-9.-]*:)?[0-9]+$ ]] || die "$name must be [host]:port: '$value'"
}

# Filesystem path with no shell/systemd metacharacters, whitespace, or newlines.
validate_path() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[A-Za-z0-9._/-]+$ ]] || die "$name contains disallowed characters: '$value'"
}

# Absolute filesystem path, additionally validated via validate_path.
validate_abs_path() {
  local name="$1" value="$2"
  [[ "$value" == /* ]] || die "$name must be an absolute path: '$value'"
  validate_path "$name" "$value"
}

# Mock event rate (--mock-rate): integer 1..1000. 0 is meaningless, and the
# agent's tokio interval period becomes 0 above 1000 and panics (#260).
# Digits-only match also rejects empty, negative, and newline-bearing values.
validate_mock_rate() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^[0-9]{1,4}$ ]] || (( 10#$value < 1 || 10#$value > 1000 )); then
    die "$name must be an integer between 1 and 1000: '$value'"
  fi
}
