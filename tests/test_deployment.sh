#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bash -n "$root/deploy/update-vps.sh"

grep -Fq '"deliveries", "claim"' "$root/src/agentd.rs"
grep -Fq '"ack",' "$root/src/agentd.rs"
grep -Fq 'repo_dir=/opt/agentd-telegram-adapter' "$root/deploy/update-vps.sh"
grep -Fq 'minifish-org/agentd-telegram-adapter.git' "$root/deploy/update-vps.sh"
! grep -R -q '/v1/turns\|delivery/outbox\|/receipt' \
  "$root/src" \
  "$root/README.md" \
  "$root/deploy"

if command -v systemd-analyze >/dev/null 2>&1; then
  systemd-analyze verify "$root/deploy/tg-adapter.service"
fi

printf 'deployment assets: ok\n'
