#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s [SSH_TARGET]\n' "$0" >&2
  exit 2
}

valid_ssh_target() {
  case "$1" in -*|*:*|*[!A-Za-z0-9._@-]*|'') return 1 ;; esac
  [[ "$1" =~ ^([A-Za-z0-9][A-Za-z0-9._-]*@)?[A-Za-z0-9][A-Za-z0-9._-]*$ ]]
}

test "$#" -le 1 || usage
target=${1:-singapore}
valid_ssh_target "$target" || { printf 'error: unsafe SSH target\n' >&2; exit 2; }

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

test "$(git -C "$repo_root" branch --show-current)" = main || {
  printf 'error: local checkout must be on main\n' >&2
  exit 1
}
test -z "$(git -C "$repo_root" status --porcelain)" || {
  printf 'error: commit or discard local changes before deploying\n' >&2
  exit 1
}
git -C "$repo_root" fetch --quiet origin main
test "$(git -C "$repo_root" rev-parse HEAD)" = "$(git -C "$repo_root" rev-parse origin/main)" || {
  printf 'error: local main must exactly match origin/main\n' >&2
  exit 1
}

ssh "$target" 'bash -se' <<'REMOTE'
set -Eeuo pipefail

repo_dir=/opt/agentd-telegram-adapter
repo_url=git@github.com:minifish-org/agentd-telegram-adapter.git
toolchain=1.92.0
service=tg-adapter.service
unit_path=/etc/systemd/system/tg-adapter.service
install_root=/usr/local/lib/agentd-telegram-adapter
current_link=$install_root/current
backup_dir=
previous_target=
previous_unit=0
was_active=0
cutover_started=0
success=0
next_link=
verify_link_created=0

rollback() {
  test "$cutover_started" -eq 1 || return 0
  printf 'deployment failed; restoring previous Telegram adapter\n' >&2
  sudo systemctl stop "$service" >/dev/null 2>&1 || true
  if test -n "$previous_target"; then
    rollback_link=$install_root/.current-rollback-$$
    sudo ln -s "$previous_target" "$rollback_link"
    sudo mv -Tf "$rollback_link" "$current_link"
  else
    sudo rm -f "$current_link"
  fi
  if test "$previous_unit" -eq 1; then
    sudo install -m 0644 "$backup_dir/unit" "$unit_path"
  else
    sudo rm -f "$unit_path"
  fi
  sudo systemctl daemon-reload
  if test "$was_active" -eq 1; then
    sudo systemctl restart "$service"
    sudo systemctl is-active --quiet "$service"
  fi
}

finish() {
  rc=$?
  trap - EXIT HUP INT TERM
  if test "$rc" -ne 0 && test "$success" -eq 0; then rollback || rc=1; fi
  test -z "$next_link" || sudo rm -f "$next_link"
  if test "$verify_link_created" -eq 1; then sudo rm -f "$current_link"; fi
  test -z "$backup_dir" || rm -rf "$backup_dir"
  exit "$rc"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if test ! -d "$repo_dir"; then
  sudo install -d -o "$(id -un)" -g "$(id -gn)" "$repo_dir"
fi
cd "$repo_dir"
if test ! -d .git; then
  git init -b main
  git remote add origin "$repo_url"
  git fetch --prune origin main
  git checkout -B main origin/main
else
  test -z "$(git status --porcelain --untracked-files=no)" || {
    printf 'error: refusing to overwrite tracked VPS checkout changes\n' >&2
    exit 1
  }
  git remote set-url origin "$repo_url"
  git fetch --prune origin main
  git checkout main
  git merge --ff-only origin/main
fi
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" || {
  printf 'error: VPS checkout must exactly match origin/main\n' >&2
  exit 1
}
test -z "$(git status --porcelain)" || {
  printf 'error: VPS checkout contains unexpected files\n' >&2
  exit 1
}
revision=$(git rev-parse HEAD)

export PATH="$HOME/.cargo/bin:$PATH"
command -v rustup >/dev/null || { printf 'error: rustup is required on the VPS\n' >&2; exit 1; }
rustup toolchain install "$toolchain" --profile minimal --no-self-update
cargo "+$toolchain" build --locked --release
artifact=$repo_dir/target/release/agentd-telegram-adapter
test -x "$artifact"
sha=$(sha256sum "$artifact" | awk '{print $1}')
version_dir=$install_root/$sha
versioned_binary=$version_dir/agentd-telegram-adapter
unit_source=$repo_dir/deploy/tg-adapter.service

test -f /etc/tg-adapter.env && test ! -L /etc/tg-adapter.env
test -f /etc/tg-adapter-decoy.env && test ! -L /etc/tg-adapter-decoy.env
test -f "$unit_source" && test ! -L "$unit_source"
sudo install -d "$version_dir"
if sudo test -e "$versioned_binary"; then
  test "$(sudo sha256sum "$versioned_binary" | awk '{print $1}')" = "$sha"
else
  sudo install -m 0755 "$artifact" "$versioned_binary.new"
  sudo mv -f "$versioned_binary.new" "$versioned_binary"
fi

if sudo test -L "$current_link"; then
  previous_target=$(sudo readlink "$current_link")
  case "$previous_target" in "$install_root"/*) ;; *) printf 'error: unsafe current symlink\n' >&2; exit 1 ;; esac
  sudo test -x "$previous_target/agentd-telegram-adapter"
elif sudo test -e "$current_link"; then
  printf 'error: current install path is not a symlink\n' >&2
  exit 1
else
  sudo ln -s "$version_dir" "$current_link"
  verify_link_created=1
fi
sudo systemd-analyze verify "$unit_source"
if test "$verify_link_created" -eq 1; then
  sudo rm -f "$current_link"
  verify_link_created=0
fi

backup_dir=$(mktemp -d)
if sudo test -f "$unit_path" && ! sudo test -L "$unit_path"; then
  sudo cp -a "$unit_path" "$backup_dir/unit"
  previous_unit=1
elif sudo test -e "$unit_path"; then
  printf 'error: existing systemd unit is not a regular file\n' >&2
  exit 1
fi
if sudo systemctl is-active --quiet "$service"; then was_active=1; fi

next_link=$install_root/.current-$sha-$$
sudo ln -s "$version_dir" "$next_link"
cutover_started=1
sudo systemctl stop "$service" >/dev/null 2>&1 || true
sudo mv -Tf "$next_link" "$current_link"
next_link=
sudo install -m 0644 "$unit_source" "$unit_path"
sudo systemctl daemon-reload
sudo systemctl enable "$service" >/dev/null
sudo systemctl start "$service"

healthy=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if sudo systemctl is-active --quiet "$service"; then
    pid=$(sudo systemctl show "$service" -p MainPID --value)
    expected_exe=$(sudo readlink -f "$versioned_binary")
    actual_exe=$(sudo readlink -f "/proc/$pid/exe" 2>/dev/null || true)
    if test "$actual_exe" = "$expected_exe" &&
      curl --fail --silent --show-error --max-time 10 http://127.0.0.1:18080/ >/dev/null 2>&1; then
      healthy=1
      break
    fi
  fi
  sleep 1
done
test "$healthy" -eq 1 || { printf 'error: adapter did not become healthy\n' >&2; false; }

success=1
printf 'deployed Telegram adapter revision=%s sha256=%s\n' "$revision" "$sha"
REMOTE
