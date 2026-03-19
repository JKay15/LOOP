#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE_ROOT="${1:-$ROOT/.loop}"

required=(
  "$STATE_ROOT/state"
  "$STATE_ROOT/cache"
  "$STATE_ROOT/audit"
  "$STATE_ROOT/artifacts"
  "$STATE_ROOT/quarantine"
)

missing=0
for path in "${required[@]}"; do
  if [[ ! -d "$path" ]]; then
    echo "[loop-state-tree][MISSING] $path" >&2
    missing=1
  else
    echo "[loop-state-tree][OK] $path"
  fi
done

exit "$missing"
