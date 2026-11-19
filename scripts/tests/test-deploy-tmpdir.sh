#!/usr/bin/env bash
# Regression test for #358: deploy scripts must stage files in a remote
# `mktemp -d` directory (no fixed /tmp path), install root-owned binaries with
# `install -o root`, and use rsync --no-links for static files. ssh/scp/sudo
# are stubbed; the commands they receive are captured. Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist" "$TMP/stubs" "$TMP/go-collector/static"
cp "$SRC/scripts/install-collector.sh" "$SRC/scripts/deploy-agent.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
: > "$TMP/dist/sentinel-collector"; : > "$TMP/dist/sentinel-agent"
: > "$TMP/go-collector/static/index.html"

cat > "$TMP/stubs/ssh" <<STUB
#!/bin/sh
echo "ssh \$*" >> "$TMP/calls"
case "\$*" in
  *mktemp*) echo "/tmp/tmp.RANDOM1234"; exit 0 ;;
  *hostname*) echo "192.0.2.1"; exit 0 ;;
  *"/dev/stdin"*|*"sudo tee"*) cat >/dev/null ;;
  *"bash -s"*) cat | tee -a "$TMP/remote-scripts" >> "$TMP/calls" ;;
esac
exit 0
STUB
printf '#!/bin/sh\necho "scp $*" >> "%s/calls"\n' "$TMP" > "$TMP/stubs/scp"
printf '#!/bin/sh\nexit 0\n' > "$TMP/stubs/sudo"
chmod +x "$TMP/stubs/"*
export PATH="$TMP/stubs:$PATH"

fail=0
has() { if grep -qF -- "$2" "$TMP/calls"; then echo "ok   $1"; else echo "FAIL $1 (missing: $2)"; fail=1; fi; }
hasnt() { if grep -qF -- "$2" "$TMP/calls"; then echo "FAIL $1 (found: $2)"; fail=1; else echo "ok   $1"; fi; }

: > "$TMP/calls"
bash "$TMP/scripts/install-collector.sh" testhost >/dev/null 2>&1 || { echo "FAIL install-collector exited non-zero"; fail=1; }
has   "collector: remote mktemp"      "ssh -n -- testhost mktemp -d"
has   "collector: scp to random dir"  "testhost:/tmp/tmp.RANDOM1234/sentinel-collector"
has   "collector: install -o root"    "install -o root -g root -m 755"
has   "collector: rsync --no-links"   "--no-links"
has   "collector: staging removed"    "rm -rf -- /tmp/tmp.RANDOM1234"
hasnt "collector: no fixed /tmp path" "/tmp/sentinel-"
hasnt "collector: no sudo mv"         "sudo mv"

: > "$TMP/calls"
bash "$TMP/scripts/deploy-agent.sh" --mock --collector 192.0.2.10:50051 testhost >/dev/null 2>&1 || { echo "FAIL deploy-agent exited non-zero"; fail=1; }
has   "agent: remote mktemp"          "ssh -n -- testhost mktemp -d"
has   "agent: scp to random dir"      "testhost:/tmp/tmp.RANDOM1234/sentinel-agent"
has   "agent: install -o root"        "install -o root -g root -m 755"
has   "agent: staging removed"        "rm -rf -- /tmp/tmp.RANDOM1234"
hasnt "agent: no fixed /tmp path"     "/tmp/sentinel-"

# rsync without --chown support: the remote script must fall back to
# --no-owner/--no-group plus an explicit chown. Run the captured remote
# script locally with stubbed rsync/sudo.
awk '/^set -euo pipefail/{n++} {print > ("'"$TMP"'/part" n)}' "$TMP/remote-scripts"
fb=""
for f in "$TMP"/part*; do grep -qF rsync "$f" && fb="$f"; done
if [[ -z "$fb" ]]; then echo "FAIL fallback: rsync script not captured"; fail=1; else
  mkdir -p "$TMP/fb/bin" "$TMP/fb/src"
  printf '#!/bin/sh\necho "usage: rsync [OPTION]... (old)"\n' > "$TMP/fb/bin/rsync"
  printf '#!/bin/sh\necho "$*" >> "%s/fb/calls"\n' "$TMP" > "$TMP/fb/bin/sudo"
  chmod +x "$TMP/fb/bin/"*
  PATH="$TMP/fb/bin:$PATH" bash "$fb" "$TMP/fb/src" /dest >/dev/null 2>&1
  if grep -q -- '--no-owner --no-group' "$TMP/fb/calls" && grep -q 'chown -R root:root /dest' "$TMP/fb/calls" \
     && ! grep -q -- '--chown=' "$TMP/fb/calls"; then echo "ok   rsync fallback without --chown"; else echo "FAIL rsync fallback"; cat "$TMP/fb/calls"; fail=1; fi
  printf '#!/bin/sh\necho "--chown supported"\n' > "$TMP/fb/bin/rsync"
  : > "$TMP/fb/calls"
  PATH="$TMP/fb/bin:$PATH" bash "$fb" "$TMP/fb/src" /dest >/dev/null 2>&1
  if grep -q -- '--chown=root:root' "$TMP/fb/calls"; then echo "ok   rsync --chown when supported"; else echo "FAIL rsync --chown path"; fail=1; fi
fi

exit "$fail"
