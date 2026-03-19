#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE_ROOT="${1:-$ROOT/.loop}"

UV_CACHE_DIR="${UV_CACHE_DIR:-$ROOT/.uv-cache}" \
  uv run --project "$ROOT" python -m loop_product.kernel.entry --smoke --state-root "$STATE_ROOT"
