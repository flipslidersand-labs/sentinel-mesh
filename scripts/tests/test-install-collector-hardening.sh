#!/usr/bin/env bash
# Regression test for #365: the generated collector unit must run as an
# unprivileged User= and carry hardening directives. ssh/scp/sudo are stubbed;
# the unit written via `sudo tee` is captured. Touches only a tmp dir.
set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts" "$TMP/dist" "$TMP/stubs"
cp "$SRC/scripts/install-collector.sh" "$SRC/scripts/lib-validate.sh" "$TMP/scripts/"
: > "$TMP/dist/sentinel-collector"

# ssh stub: log args and stdin; capture the unit when it is piped to tee.
cat > "$TMP/stubs/ssh" <<STUB
#!/bin/sh
echo "ssh \$*" >> "$TMP/ssh-calls"
case "\$*" in
  *hostname*) echo "192.0.2.1"; exit 0 ;;
  *mktemp*) echo "/tmp/tmp.RANDOM1234"; exit 0 ;;
  *tee*) cat > "$TMP/unit" ;;
  *"bash -s"*) cat >> "$TMP/remote-scripts" ;;
esac
exit 0
STUB
for cmd in scp sudo; do
  printf '#!/bin/sh\nexit 0\n' > "$TMP/stubs/$cmd"
done
chmod +x "$TMP/stubs/"*
export PATH="$TMP/stubs:$PATH"

fail=0
check() { # label, file, fixed string
  if grep -qF -- "$3" "$2" 2>/dev/null; then echo "ok   $1"; else echo "FAIL $1 (missing: $3)"; fail=1; fi
}

out="$(bash "$TMP/scripts/install-collector.sh" testhost --data-dir /srv/sm-data 2>&1)" \
  || { echo "FAIL installer exited non-zero: $out"; fail=1; }

check "User="                 "$TMP/unit" "User=sentinel"
check "Group="                "$TMP/unit" "Group=sentinel"
check "NoNewPrivileges"       "$TMP/unit" "NoNewPrivileges=yes"
check "ProtectSystem=strict"  "$TMP/unit" "ProtectSystem=strict"
check "ProtectHome"           "$TMP/unit" "ProtectHome=yes"
check "PrivateTmp"            "$TMP/unit" "PrivateTmp=yes"
check "ReadWritePaths=data"   "$TMP/unit" "ReadWritePaths=/srv/sm-data"
check "bind capability"       "$TMP/unit" "AmbientCapabilities=CAP_NET_BIND_SERVICE"
check "ExecStart data-dir"    "$TMP/unit" "--data-dir /srv/sm-data"
if grep -q '^User=root' "$TMP/unit" 2>/dev/null; then echo "FAIL runs as root"; fail=1; fi

# Installer must create the user and chown the data dir (migration).
check "useradd"  "$TMP/remote-scripts" "useradd --system"
check "chown"    "$TMP/remote-scripts" 'chown -R "$user": "$data"'
check "data dir passed to remote" "$TMP/ssh-calls" "sentinel /srv/sm-data"

# Dangerous --data-dir values must be rejected before any ssh is reached.
for bad in / /var/lib /var/lib/ /opt /etc /usr /usr/lib /home/x /var/lib/../etc /srv//x; do
  : > "$TMP/ssh-calls"
  if bash "$TMP/scripts/install-collector.sh" testhost --data-dir "$bad" >/dev/null 2>&1; then
    echo "FAIL accepted --data-dir $bad"; fail=1
  elif [[ -s "$TMP/ssh-calls" ]]; then
    echo "FAIL ssh reached for --data-dir $bad"; fail=1
  else
    echo "ok   reject --data-dir $bad"
  fi
done
for good in /var/lib/sentinel /srv/sm-data /opt/sentinel/data; do
  if bash "$TMP/scripts/install-collector.sh" testhost --data-dir "$good" >/dev/null 2>&1; then
    echo "ok   accept --data-dir $good"
  else
    echo "FAIL rejected --data-dir $good"; fail=1
  fi
done
check "stop before chown" "$TMP/remote-scripts" "systemctl stop sentinel-collector"
check "user-group"        "$TMP/remote-scripts" "--user-group"

if command -v systemd-analyze >/dev/null 2>&1 && [[ -s "$TMP/unit" ]]; then
  # ExecStart binary is absent locally; only flag syntax/directive errors.
  va="$(systemd-analyze verify "$TMP/unit" 2>&1 | grep -iE 'unknown (key|section)|Unknown lvalue|Invalid|Failed to parse' || true)"
  if [[ -n "$va" ]]; then echo "FAIL systemd-analyze: $va"; fail=1; else echo "ok   systemd-analyze verify"; fi
fi

exit "$fail"
