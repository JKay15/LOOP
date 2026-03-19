#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE_ROOT="${1:-$ROOT/.loop}"
AUDIT_DIR="$STATE_ROOT/audit"

if [[ ! -d "$AUDIT_DIR" ]]; then
  echo "[loop-audit-tail][MISSING] $AUDIT_DIR" >&2
  exit 1
fi

latest_file="$(find "$AUDIT_DIR" -maxdepth 1 -type f | sort | tail -n 1)"
if [[ -z "$latest_file" ]]; then
  echo "[loop-audit-tail][EMPTY] no audit files under $AUDIT_DIR"
  exit 0
fi

tail -n "${2:-40}" "$latest_file"
