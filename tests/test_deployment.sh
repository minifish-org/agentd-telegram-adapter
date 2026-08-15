#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bash -n "$root/deploy/update-vps.sh"

grep -Fq '"deliveries", "claim"' "$root/src/agentd.rs"
grep -Fq '"ack",' "$root/src/agentd.rs"
grep -Fq 'repo_dir=/opt/agentd-telegram-adapter' "$root/deploy/update-vps.sh"
grep -Fq 'test "$#" -eq 1 || usage' "$root/deploy/update-vps.sh"
grep -Fq 'repo_url=https://github.com/minifish-org/agentd-telegram-adapter.git' \
  "$root/deploy/update-vps.sh"
! grep -R -q 'minifish-home\|taila2cd17\|target=${1:-singapore}' \
  "$root/src" \
  "$root/README.md" \
  "$root/deploy"
! grep -R -q '/v1/turns\|delivery/outbox\|/receipt' \
  "$root/src" \
  "$root/README.md" \
  "$root/deploy"
! grep -R -q '/tools/execute\|call_tool' "$root/src" "$root/tests"

if command -v systemd-analyze >/dev/null 2>&1; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  sed 's|^ExecStart=.*|ExecStart=/bin/true|' \
    "$root/deploy/tg-adapter.service" > "$tmp/tg-adapter.service"
  systemd-analyze verify "$tmp/tg-adapter.service"
fi

printf 'deployment assets: ok\n'
