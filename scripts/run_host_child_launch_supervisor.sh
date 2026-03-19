#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

exec uv run --locked python -m loop_product.host_child_launch_supervisor --repo-root "$ROOT_DIR" "$@"
