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
